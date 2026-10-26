//! stream_exec.zig — streaming query executor
//!
//! Executes a kqq Query against a JSON stream without buffering the whole document.
//!
//! Algorithm for  [users.*] select name, score where active = true order by score desc limit 10
//!
//!   1. Navigate the stream to the "users" array by matching the scope prefix path.
//!      Everything before the target array is consumed and discarded.
//!   2. For each element of the array:
//!      a. Collect all its top-level fields into a small HashMap<str,JsonValue>.
//!         Values are fully owned copies (strings duplicated).
//!      b. Evaluate WHERE against the map.
//!      c. If passes: project SELECT fields, append to `matches`.
//!      d. If no order_by AND limit reached: stop reading entirely.
//!   3. After all matching records collected:
//!      a. Sort by order_by if present.
//!      b. Apply limit.
//!      c. Emit as JSON array to stdout.
//!
//! Memory usage:
//!   - No order_by + limit N  → O(N) records held at once
//!   - No order_by + no limit → O(output) — emitted as records arrive (TODO: future streaming emit)
//!   - order_by present       → O(matches) after WHERE filter
//!   - Input document         → never fully in RAM

const std = @import("std");
const query = @import("kqq_query");
const stream = @import("kqq_stream");
const regex = @import("kqq_regex");
const record_mod = @import("kqq_record");
const expr_mod = @import("kqq_expr");
const where_mod = @import("kqq_where");
const output_mod = @import("kqq_output");
const llm_mod = @import("kqq_llm");
const raw_where_mod = @import("kqq_raw_where");

const simpleRegexMatch = regex.simpleRegexMatch;
const globLike = regex.globLike;

// Re-export record types for backward compatibility (record_source.zig, main.zig, etc.)
pub const OwnedValue = record_mod.OwnedValue;
pub const Number = record_mod.Number;
pub const isJsonNumber = stream.isJsonNumber;
pub const Record = record_mod.Record;
pub const PATH_SEP = record_mod.PATH_SEP;
const copyRecord = record_mod.copyRecord;
const copyRecordFull = record_mod.copyRecordFull;
const compareRecords = record_mod.compareRecords;

// Expression evaluator (from expr.zig)
const evalExpr = expr_mod.evalExpr;
const evalFunc = expr_mod.evalFunc;
const toF64 = expr_mod.toF64;
const numericBinOp = expr_mod.numericBinOp;
const appendJsonStr = expr_mod.appendJsonStr;
const appendOwnedJson = expr_mod.appendOwnedJson;
const appendParsedJson = expr_mod.appendParsedJson;

// WHERE evaluation (from where.zig)
const ownedMatchesCond = where_mod.ownedMatchesCond;
const recordPassesWhere = where_mod.recordPassesWhere;
const condOnRecord = where_mod.condOnRecord;

// Output formatting (from output.zig)
pub const OutputFormat = output_mod.OutputFormat;
const emitStreamingRecord = output_mod.emitStreamingRecord;
const emitResults = output_mod.emitResults;
const writeOwnedValue = output_mod.writeOwnedValue;
const writeOwnedValueRaw = output_mod.writeOwnedValueRaw;
const writeCsvField = output_mod.writeCsvField;
const writeJsonEscaped = output_mod.writeJsonEscaped;
const writeNumber = output_mod.writeNumber;
const writeRecordJson = output_mod.writeRecordJson;
const emitRecordNdjsonLine = output_mod.emitRecordNdjsonLine;

// LLM helpers (from llm.zig)
pub const ApiMode = llm_mod.ApiMode;
pub const SchemaFieldType = llm_mod.SchemaFieldType;
pub const SchemaField = llm_mod.SchemaField;
pub const parseExpectSchema = llm_mod.parseExpectSchema;
const stripSseLine = llm_mod.stripSseLine;
const extractNestedField = llm_mod.extractNestedField;
const validateSchema = llm_mod.validateSchema;
const extractNextObject = llm_mod.extractNextObject;
const drainAccum = llm_mod.drainAccum;
const emitGroupBySnapshot = llm_mod.emitGroupBySnapshot;
const writeOneJsonRecord = llm_mod.writeOneJsonRecord;

// Raw-byte WHERE fast path (from raw_where.zig)
const rawScanField = raw_where_mod.rawScanField;
const rawSkipValue = raw_where_mod.rawSkipValue;
const rawValueCmp = raw_where_mod.rawValueCmp;
const tryWhereOnRaw = raw_where_mod.tryWhereOnRaw;
const tryCondOnRaw = raw_where_mod.tryCondOnRaw;

const SelectField = query.SelectField;
const WhereClause = query.WhereClause;
const OrderField = query.OrderField;
const Op = query.Op;
const Value = query.Value;
const SortDir = query.SortDir;
const Expr = query.Expr;
const FuncName = query.FuncName;
const AggFunc = query.AggFunc;

// ─── Execution options ───────────────────────────────────────────────────────

pub const ExecOptions = struct {
    /// When true and exactly one field is selected, emit bare unquoted values
    /// (one per line) instead of a JSON array of objects.
    raw: bool = false,
    /// Output format: json (default), csv, tsv, raw
    format: OutputFormat = .json,
    /// If set, every input record (matching OR non-matching) is written here as-is.
    /// Implements `--tee <file>` — full copy of the raw stream before filtering.
    tee_writer: ?*std.Io.Writer = null,
    /// If set, records that do NOT match the WHERE clause are written here as-is.
    /// Implements `--reject <file>` — the complement route of the filter.
    reject_writer: ?*std.Io.Writer = null,
    /// If set, activates LLM stream mode: each NDJSON envelope line has a token
    /// fragment in this field (e.g. "response" for Ollama). Fragments are accumulated
    /// and complete JSON objects are extracted and queried as they form.
    llm_field: ?[]const u8 = null,
    /// When true (--rolling), aggregate queries in LLM mode emit a rolling snapshot
    /// after each new object is incorporated — one NDJSON line per update.
    rolling: bool = false,
    /// Dotted path for nested field extraction (e.g. "choices.0.delta.content").
    /// Used with --api openai/anthropic or standalone --llm-path.
    llm_path: ?[]const u8 = null,
    /// API protocol mode — controls SSE parsing and field extraction defaults.
    api_mode: ?ApiMode = null,
    /// Schema validation fields from --expect.
    expect_schema: ?[]const SchemaField = null,
    /// If non-null, incremented each time a record is skipped due to malformed
    /// JSON or a parse error. Caller can use this to emit a warning summary.
    skipped_records: ?*usize = null,
};

// ─── Project a Record into a projected Record ────────────────────────────────

/// Return the last dot-segment of a key, e.g. "address.city" → "city".
/// Used as the default output field name for nested selects.
fn leafKey(key: []const u8) []const u8 {
    const dot = std.mem.lastIndexOfScalar(u8, key, '.') orelse return key;
    return key[dot + 1 ..];
}

fn projectRecord(
    allocator: std.mem.Allocator,
    src: *const Record,
    fields: []const SelectField,
) !Record {
    var out = Record.init(allocator);
    errdefer out.deinit();

    // Collect the set of keys to REMOVE (is_remove fields) so we can skip them
    // when copying * or when they appear in the base list.
    var remove_set = std.StringHashMap(void).init(allocator);
    defer remove_set.deinit();
    for (fields) |f| {
        if (f.is_remove) try remove_set.put(f.key, {});
    }

    for (fields) |f| {
        // Skip REMOVE directives — they only affect the remove_set above.
        if (f.is_remove) continue;
        // Skip EXPAND directives at this level — handled by caller (execStreamNDJSON).
        if (f.is_expand) continue;

        // "*" wildcard — copy all top-level (non-dotted) fields, respecting remove_set
        if (std.mem.eql(u8, f.key, "*")) {
            var it = src.fields.iterator();
            while (it.next()) |e| {
                // Skip intermediate path keys (e.g. "address\x1fcity" when "address" also exists).
                // Literal dotted keys like "svc.metric" have no PATH_SEP so they are emitted.
                if (std.mem.indexOfScalar(u8, e.key_ptr.*, PATH_SEP) != null) continue;
                // Skip removed fields
                if (remove_set.contains(e.key_ptr.*)) continue;
                // Skip if already set (e.g. by an earlier is_add column)
                if (out.fields.contains(e.key_ptr.*)) continue;
                const ok = try allocator.dupe(u8, e.key_ptr.*);
                errdefer allocator.free(ok);
                const ov = try e.value_ptr.*.copy(allocator);
                try out.fields.put(allocator, ok, ov);
            }
            continue;
        }
        // Computed column via expression (e.g. `select price * qty as total`)
        // or ADD column (select * add price*qty as total)
        if (f.expr) |e| {
            const out_key = try allocator.dupe(u8, if (f.alias) |a| a else f.key);
            errdefer allocator.free(out_key);
            // evalExpr already allocates into `allocator` — take ownership directly.
            // An expression that cannot be evaluated (missing field, type
            // mismatch) is SQL NULL: the column is still emitted, as `null`.
            const val = evalExpr(allocator, src, e) orelse .null_val;
            try out.fields.put(allocator, out_key, val);
            continue;
        }
        const ov_opt = src.get(f.key);
        // Apply isnull default: field missing or null → use default string if set
        const ov: OwnedValue = if (ov_opt) |v| blk: {
            if (v == .null_val) {
                if (f.default_val) |def| break :blk OwnedValue{ .string = @constCast(def) };
            }
            break :blk v;
        } else if (f.default_val) |def|
            OwnedValue{ .string = @constCast(def) }
        else
            .null_val;
        // Output key: explicit alias > leaf segment of dotted path > plain key
        const raw_key = if (f.alias) |a| a else leafKey(f.key);
        const out_key = try allocator.dupe(u8, raw_key);
        errdefer allocator.free(out_key);
        const out_val = try ov.copy(allocator);
        try out.fields.put(allocator, out_key, out_val);
    }
    return out;
}

// ─── Collect one object from the scanner into a Record ───────────────────────
//
// Caller has already consumed the opening `{`.
// This function reads until the matching `}`, collecting field→value pairs.
//
// Nested objects are flattened with dot-separated keys so that
// `where user.address.city = "London"` works on {"user":{"address":{"city":"London"}}}.
//
// Arrays are stored as `.raw` JSON strings (querying inside arrays is future work).

fn collectRecord(
    allocator: std.mem.Allocator,
    sc: anytype,
) !Record {
    var rec = Record.init(allocator);
    errdefer rec.deinit();
    try collectObjectInto(allocator, sc, &rec, "");
    return rec;
}

/// Recursively collect a JSON object (opening `{` already consumed) into `rec`.
/// `prefix` is the dot path to prepend to each key (empty string at the top level).
/// The scanner guarantees well-formed structure, so inside an object only keys
/// and the closing brace can appear; malformed or truncated input surfaces as
/// a scanner error rather than an unexpected event.
fn collectObjectInto(
    allocator: std.mem.Allocator,
    sc: anytype,
    rec: *Record,
    prefix: []const u8,
) !void {
    while (true) {
        const ev = try sc.next();
        const k = switch (ev) {
            .object_end => return,
            .key => |k| k,
            else => return error.BadJson,
        };
        // Build the full path key: prefix + PATH_SEP + k  (or just k at root).
        // PATH_SEP ('\x1f') is used instead of '.' so that JSON keys that
        // literally contain dots (e.g. {"svc.metric": 1}) are stored as-is
        // and do not collide with nested-path keys.
        const full_key: []u8 = if (prefix.len == 0)
            try allocator.dupe(u8, k)
        else
            try std.fmt.allocPrint(allocator, "{s}{c}{s}", .{ prefix, PATH_SEP, k });
        errdefer allocator.free(full_key);

        const vev = try sc.next();
        switch (vev) {
            .object_start => {
                // Store the whole nested object as `.raw` for output fidelity,
                // AND recurse to populate flattened dot-path keys for querying.
                var raw_buf = std.array_list.Managed(u8).init(allocator);
                errdefer raw_buf.deinit();
                try raw_buf.append('{');
                try collectNestedObject(allocator, sc, rec, full_key, &raw_buf);
                const raw_bytes = try raw_buf.toOwnedSlice();
                try rec.fields.put(allocator, full_key, .{ .raw = raw_bytes });
            },
            .array_start => {
                // Arrays: store raw, plus indexed entries key\x1f0, key\x1f1, ...
                var raw_buf = std.array_list.Managed(u8).init(allocator);
                errdefer raw_buf.deinit();
                try raw_buf.append('[');
                try collectRawArray(sc, &raw_buf);
                const raw_bytes = try raw_buf.toOwnedSlice();
                if (std.json.parseFromSlice(std.json.Value, allocator, raw_bytes, .{})) |parsed| {
                    defer parsed.deinit();
                    if (parsed.value == .array) {
                        for (parsed.value.array.items, 0..) |elem, idx| {
                            const idx_key = try std.fmt.allocPrint(allocator, "{s}{c}{d}", .{ full_key, PATH_SEP, idx });
                            errdefer allocator.free(idx_key);
                            try rec.fields.put(allocator, idx_key, try ownedFromJson(allocator, elem));
                        }
                    }
                } else |_| {} // raw_buf is valid JSON by construction; nothing to index otherwise
                try rec.fields.put(allocator, full_key, .{ .raw = raw_bytes });
            },
            .string => |sv| try rec.fields.put(allocator, full_key, .{ .string = try allocator.dupe(u8, sv) }),
            .number => |n| try rec.fields.put(allocator, full_key, .{ .number = try Number.fromLiteral(allocator, n.value, n.text) }),
            .boolean => |b| try rec.fields.put(allocator, full_key, .{ .boolean = b }),
            .null_val => try rec.fields.put(allocator, full_key, .null_val),
            else => return error.BadJson,
        }
    }
}

/// Convert a std.json value (an array element) into an owned field value.
/// Integers that an f64 cannot hold exactly keep their literal text.
fn ownedFromJson(allocator: std.mem.Allocator, v: std.json.Value) !OwnedValue {
    return switch (v) {
        .string => |sv| .{ .string = try allocator.dupe(u8, sv) },
        .integer => |n| blk: {
            var tmp: [24]u8 = undefined;
            const text = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch unreachable;
            break :blk .{ .number = try Number.fromLiteral(allocator, @floatFromInt(n), text) };
        },
        .float => |f| OwnedValue.num(f),
        .number_string => |text| .{ .number = try Number.fromLiteral(
            allocator,
            std.fmt.parseFloat(f64, text) catch std.math.nan(f64),
            text,
        ) },
        .bool => |b| .{ .boolean = b },
        .null => .null_val,
        .array, .object => blk: {
            var aw = std.Io.Writer.Allocating.init(allocator);
            errdefer aw.deinit();
            try std.json.Stringify.value(v, .{}, &aw.writer);
            break :blk .{ .raw = try aw.toOwnedSlice() };
        },
    };
}

/// Append a scalar event to a raw JSON buffer: strings re-escaped, numbers by
/// their source text (exact, and never reformatted).
fn appendRawScalar(buf: *std.array_list.Managed(u8), ev: stream.Event) !void {
    switch (ev) {
        .string => |sv| try appendJsonStr(buf, sv),
        .number => |n| try buf.appendSlice(n.text),
        .boolean => |b| try buf.appendSlice(if (b) "true" else "false"),
        .null_val => try buf.appendSlice("null"),
        else => unreachable,
    }
}

/// Collect a nested object that was already entered (opening `{` consumed).
/// Simultaneously:
///   - appends raw JSON bytes to `raw_buf` (for output)
///   - inserts flattened dot-path entries into `rec` for every member; nested
///     objects and arrays are also stored whole as `.raw`
/// The full_key for the object itself is inserted by the caller after we return.
fn collectNestedObject(
    allocator: std.mem.Allocator,
    sc: anytype,
    rec: *Record,
    prefix: []const u8,
    raw_buf: *std.array_list.Managed(u8),
) !void {
    var first = true;
    while (true) {
        const ev = try sc.next();
        const k = switch (ev) {
            .object_end => {
                try raw_buf.append('}');
                return;
            },
            .key => |k| k,
            else => return error.BadJson,
        };
        if (!first) try raw_buf.appendSlice(", ");
        first = false;
        try appendJsonStr(raw_buf, k);
        try raw_buf.appendSlice(": ");

        const child_key = try std.fmt.allocPrint(allocator, "{s}{c}{s}", .{ prefix, PATH_SEP, k });
        errdefer allocator.free(child_key);

        const vev = try sc.next();
        switch (vev) {
            .object_start, .array_start => {
                // Write the child into the parent's raw output, then keep a
                // copy of just the child's bytes as its own `.raw` value.
                const start = raw_buf.items.len;
                if (vev == .object_start) {
                    try raw_buf.append('{');
                    try collectNestedObject(allocator, sc, rec, child_key, raw_buf);
                } else {
                    try raw_buf.append('[');
                    try collectRawArray(sc, raw_buf);
                }
                try rec.fields.put(allocator, child_key, .{ .raw = try allocator.dupe(u8, raw_buf.items[start..]) });
            },
            .string => |sv| {
                try appendRawScalar(raw_buf, vev);
                try rec.fields.put(allocator, child_key, .{ .string = try allocator.dupe(u8, sv) });
            },
            .number => |n| {
                try appendRawScalar(raw_buf, vev);
                try rec.fields.put(allocator, child_key, .{ .number = try Number.fromLiteral(allocator, n.value, n.text) });
            },
            .boolean => |b| {
                try appendRawScalar(raw_buf, vev);
                try rec.fields.put(allocator, child_key, .{ .boolean = b });
            },
            .null_val => {
                try appendRawScalar(raw_buf, vev);
                try rec.fields.put(allocator, child_key, .null_val);
            },
            else => return error.BadJson,
        }
    }
}

/// Consume and append a JSON array (opening `[` already consumed, `[` already appended to raw_buf).
fn collectRawArray(sc: anytype, raw_buf: *std.array_list.Managed(u8)) !void {
    var depth: usize = 1;
    var first = true; // no element/member written yet at the current level
    while (depth > 0) {
        const ev = try sc.next();
        switch (ev) {
            .object_end, .array_end => {
                depth -= 1;
                try raw_buf.append(if (ev == .object_end) '}' else ']');
                first = false;
            },
            .key => |ik| {
                if (!first) try raw_buf.appendSlice(", ");
                try appendJsonStr(raw_buf, ik);
                try raw_buf.appendSlice(": ");
                first = true; // the member's value follows without a separator
            },
            else => {
                if (!first) try raw_buf.appendSlice(", ");
                switch (ev) {
                    .object_start, .array_start => {
                        try raw_buf.append(if (ev == .object_start) '{' else '[');
                        depth += 1;
                        first = true;
                    },
                    .end_of_input => return error.UnexpectedEof,
                    else => {
                        try appendRawScalar(raw_buf, ev);
                        first = false;
                    },
                }
            },
        }
    }
}

// ─── Navigate to target array in the stream ──────────────────────────────────
//
// Consumes events until we've descended into the array identified by `path`.
// e.g. path = "users" → consume { key="users" [ and return
//      path = "data.records" → consume { key="data" { key="records" [ and return
// Returns false if the path was not found (end of input reached).

fn navigateToArray(sc: anytype, path: []const u8) !bool {
    // Split path into segments
    var segs: [16][]const u8 = undefined;
    var seg_count: usize = 0;
    {
        var rest = path;
        while (rest.len > 0 and seg_count < segs.len) {
            const dot = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
            segs[seg_count] = rest[0..dot];
            seg_count += 1;
            rest = if (dot < rest.len) rest[dot + 1 ..] else "";
        }
    }

    var seg_idx: usize = 0; // which segment we're looking for next

    while (true) {
        const ev = try sc.next();
        switch (ev) {
            .end_of_input => return false,
            .key => |k| {
                if (seg_idx < seg_count and std.mem.eql(u8, k, segs[seg_idx])) {
                    seg_idx += 1;
                    if (seg_idx == seg_count) {
                        // Next event should be array_start
                        const next_ev = try sc.next();
                        if (next_ev == .array_start) return true;
                        // If it's an object and there are more segs, keep going
                        if (next_ev == .object_start) {
                            seg_idx = seg_count;
                            continue;
                        }
                        return false;
                    }
                    // More segments: expect an object_start next
                    const nev = try sc.next();
                    if (nev != .object_start) return false;
                } else {
                    // Not our key — skip the value
                    try sc.skipValue();
                }
            },
            .object_start, .array_start => {}, // depth tracking handled by scanner
            .object_end, .array_end => {},
            else => {},
        }
    }
}

// ─── Main streaming executor ─────────────────────────────────────────────────

/// Execute a scoped query against a streaming JSON reader.
/// Writes a JSON array (or pretty-printed object for [*]) to `writer`.
/// Returns the number of records emitted.
pub fn execStream(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    var sc = try stream.Scanner.init(allocator, reader);
    defer sc.deinit();

    const scope = q.scope_pattern orelse {
        // No scope in streaming mode: not useful, caller should use buffered path
        return error.NoScopeInStreamMode;
    };

    // For [*] we'd need to know the schema in advance; for now require explicit scope
    if (std.mem.eql(u8, scope, "*")) return error.AutoScopeNotSupportedInStreamMode;

    // Derive navigation path: "users.*" → "users"
    const star_pos = std.mem.lastIndexOfScalar(u8, scope, '*') orelse scope.len;
    const nav_path = if (star_pos >= 2 and scope[star_pos - 1] == '.')
        scope[0 .. star_pos - 1]
    else
        scope;

    // Navigate to the target array
    const found = try navigateToArray(&sc, nav_path);
    if (!found) {
        try writer.writeAll("[]\n");
        return 0;
    }

    // Stop reading early only when every match yields exactly one output row.
    const early_stop = canStopEarly(q);
    const lim = q.limit orelse std.math.maxInt(usize);

    // Collect full (unprojected) records that pass WHERE — needed so ORDER BY
    // can sort on the original dotted field names (e.g. "address.city").
    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    // Arena for temporary per-record allocations.
    // Discarded records (WHERE fails) get their memory reclaimed by reset().
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Iterate array elements
    array_loop: while (!(early_stop and matches.items.len >= lim)) {
        const ev = try sc.next();
        switch (ev) {
            .array_end, .end_of_input => break :array_loop,
            .object_start => {
                _ = scratch.reset(.retain_capacity);
                var rec = try collectRecord(scratch.allocator(), &sc);

                // WHERE filter
                if (q.where) |w| {
                    if (!recordPassesWhere(scratch.allocator(), &rec, w)) continue :array_loop;
                }

                // Copy full record into long-lived allocator (projection happens after sort)
                try matches.append(try copyRecordFull(allocator, &rec));
            },
            else => {},
        }
    }

    return finalizeMatches(allocator, matches.items, q, writer, opts);
}

/// True when LIMIT may stop reading input as soon as enough records matched:
/// no ORDER BY (needs every record), no DISTINCT (duplicates do not count),
/// no EXPAND (one record can become several rows).
fn canStopEarly(q: query.Query) bool {
    return q.limit != null and q.order_by == null and !q.distinct and expandField(q.fields) == null;
}

/// The EXPAND directive of a SELECT list, if any.
fn expandField(fields: ?[]const SelectField) ?SelectField {
    for (fields orelse return null) |f| {
        if (f.is_expand) return f;
    }
    return null;
}

/// DISTINCT bookkeeping: remembers a fingerprint of every row emitted so far.
const DistinctSet = struct {
    seen: std.StringHashMap(void),
    buf: std.array_list.Managed(u8),

    fn init(allocator: std.mem.Allocator) DistinctSet {
        return .{
            .seen = std.StringHashMap(void).init(allocator),
            .buf = std.array_list.Managed(u8).init(allocator),
        };
    }

    fn deinit(self: *DistinctSet) void {
        var it = self.seen.keyIterator();
        while (it.next()) |k| self.seen.allocator.free(k.*);
        self.seen.deinit();
        self.buf.deinit();
    }

    /// True the first time a row with these fields and values is offered.
    fn firstSeen(self: *DistinctSet, rec: *const Record) !bool {
        self.buf.clearRetainingCapacity();
        var it = rec.fields.iterator();
        while (it.next()) |e| {
            try appendValueKey(&self.buf, .{ .string = @constCast(e.key_ptr.*) });
            try appendValueKey(&self.buf, e.value_ptr.*);
        }
        if (self.seen.contains(self.buf.items)) return false;
        const owned = try self.seen.allocator.dupe(u8, self.buf.items);
        errdefer self.seen.allocator.free(owned);
        try self.seen.put(owned, {});
        return true;
    }
};

/// Writes result rows as they are produced (no ORDER BY / EXPAND / CSV),
/// applying DISTINCT and counting emitted rows for LIMIT.
const RowEmitter = struct {
    writer: *std.Io.Writer,
    fmt: OutputFormat,
    distinct: ?DistinctSet,
    count: usize = 0,
    json_first: bool = true,

    fn init(allocator: std.mem.Allocator, writer: *std.Io.Writer, fmt: OutputFormat, distinct: bool) RowEmitter {
        return .{ .writer = writer, .fmt = fmt, .distinct = if (distinct) DistinctSet.init(allocator) else null };
    }

    fn deinit(self: *RowEmitter) void {
        if (self.distinct) |*d| d.deinit();
    }

    /// Emit a projected row unless DISTINCT has already seen it.
    fn emitRecord(self: *RowEmitter, rec: *Record) !void {
        if (self.distinct) |*d| {
            if (!try d.firstSeen(rec)) return;
        }
        try emitStreamingRecord(self.writer, rec, self.fmt, &self.json_first);
        self.count += 1;
    }

    /// Emit an input line verbatim (SELECT * / bare passthrough).
    fn emitLine(self: *RowEmitter, line: []const u8) !void {
        switch (self.fmt) {
            .json => {
                try self.writer.writeAll(if (self.json_first) "[\n  " else ",\n  ");
                self.json_first = false;
                try self.writer.writeAll(line);
            },
            .ndjson, .raw => {
                try self.writer.writeAll(line);
                try self.writer.writeByte('\n');
            },
            .csv, .tsv => unreachable,
        }
        self.count += 1;
    }

    /// Close the JSON array wrapper (other formats need no trailer).
    fn finish(self: *RowEmitter) !void {
        if (self.fmt == .json) try self.writer.writeAll(if (self.count == 0) "[]\n" else "\n]\n");
    }
};

/// Post-process buffered matches: ORDER BY → project (+EXPAND) → DISTINCT →
/// LIMIT (on output rows) → emit. Returns the number of rows emitted.
fn finalizeMatches(
    allocator: std.mem.Allocator,
    matches: []Record,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.order_by) |order| sortRecords(matches, order);

    const sel = if (q.fields) |f| @as(?[]const SelectField, f) else null;
    const lim = q.limit orelse std.math.maxInt(usize);
    const expand = expandField(sel);

    var out_recs = std.array_list.Managed(Record).init(allocator);
    defer {
        for (out_recs.items) |*r| r.deinit();
        out_recs.deinit();
    }
    var distinct: ?DistinctSet = if (q.distinct) DistinctSet.init(allocator) else null;
    defer if (distinct) |*d| d.deinit();

    // Rows produced by one match: usually one, several under EXPAND.
    var rows = std.array_list.Managed(Record).init(allocator);
    defer {
        for (rows.items) |*r| r.deinit();
        rows.deinit();
    }

    for (matches) |*rec| {
        if (out_recs.items.len >= lim) break;
        if (expand) |ef| {
            try expandRecord(allocator, rec, sel, ef, &rows);
        } else {
            try rows.append(if (sel) |s| try projectRecord(allocator, rec, s) else try copyRecord(allocator, rec));
        }
        for (rows.items) |*row| {
            const keep = out_recs.items.len < lim and
                (if (distinct) |*d| try d.firstSeen(row) else true);
            if (keep) {
                try out_recs.append(row.*);
                row.* = Record.init(allocator); // moved; leave an empty record behind
            } else {
                row.deinit();
            }
        }
        rows.clearRetainingCapacity();
    }

    try emitResults(out_recs.items, out_recs.items.len, writer, opts.format, opts.raw);
    return out_recs.items.len;
}

/// EXPAND: one output row per element of the array field `ef` (or of
/// split(field, delim)), each carrying the other projected columns.
fn expandRecord(
    allocator: std.mem.Allocator,
    rec: *const Record,
    sel: ?[]const SelectField,
    ef: SelectField,
    rows: *std.array_list.Managed(Record),
) !void {
    var split_arena = std.heap.ArenaAllocator.init(allocator);
    defer split_arena.deinit();
    const field_ov = rec.get(ef.key) orelse return;
    const field_str: []const u8 = switch (field_ov) {
        .string => |sv| sv,
        .raw => |r| r,
        else => return,
    };
    // For expand(split(field, delim)), build the JSON array by splitting.
    const arr_json: []const u8 = if (ef.expand_split_delim) |delim| blk: {
        var arr_buf = std.array_list.Managed(u8).init(split_arena.allocator());
        try arr_buf.append('[');
        var it_sp = std.mem.splitSequence(u8, field_str, delim);
        var first_elem = true;
        while (it_sp.next()) |tok_str| {
            if (!first_elem) try arr_buf.append(',');
            first_elem = false;
            try appendJsonStr(&arr_buf, tok_str);
        }
        try arr_buf.append(']');
        break :blk arr_buf.items;
    } else field_str;

    var parsed = std.json.parseFromSlice(std.json.Value, allocator, arr_json, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .array) return;
    for (parsed.value.array.items) |elem| {
        // Project non-expand fields from the current record, then add the element
        var out_rec: Record = if (sel) |s|
            try projectRecord(allocator, rec, s)
        else
            try copyRecord(allocator, rec);
        errdefer out_rec.deinit();
        const exp_key = try allocator.dupe(u8, if (ef.alias) |a| a else ef.key);
        errdefer allocator.free(exp_key);
        var exp_val = try ownedFromJson(allocator, elem);
        errdefer exp_val.deinit(allocator);
        try out_rec.fields.put(allocator, exp_key, exp_val);
        try rows.append(out_rec);
    }
}

// ─── NDJSON streaming executor ───────────────────────────────────────────────
//
// NDJSON (Newline-Delimited JSON) = one JSON object per line, no wrapping array.
// Example:
//   {"id":1,"name":"Alice","active":true}
//   {"id":2,"name":"Bob","active":false}
//
// No scope needed — every top-level object is a record.
// All SELECT / WHERE / ORDER BY / LIMIT semantics are identical.

pub fn execStreamNDJSON(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const sel = if (q.fields) |f| @as(?[]const SelectField, f) else null;
    const fmt = if (opts.raw) OutputFormat.raw else opts.format;
    const lim = q.limit orelse std.math.maxInt(usize);

    // Streaming emit: unless something needs the whole match set first —
    // ORDER BY, EXPAND, or CSV/TSV (the header comes from the first row) —
    // emit each row as it arrives, straight from the scratch arena: O(1)
    // memory (O(distinct rows) under DISTINCT), and LIMIT stops reading.
    const streaming_emit = q.order_by == null and expandField(sel) == null and
        fmt != .csv and fmt != .tsv;
    var emitter = RowEmitter.init(allocator, writer, fmt, q.distinct);
    defer emitter.deinit();

    // Buffered path only: collect full records for ORDER BY / EXPAND / CSV
    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }
    const buffered_early_stop = canStopEarly(q);

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Read line-by-line so we can access the raw bytes for --tee / --reject routing.
    // Each NDJSON record is exactly one line; we parse it with a fixed-slice sub-scanner.
    // For very long lines (> reader buffer), we fall back to a heap-allocated line_buf.
    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();

    ndjson_loop: while (if (streaming_emit) emitter.count < lim else !(buffered_early_stop and matches.items.len >= lim)) {
        // About to wait for more input: push out what was emitted so far, so a
        // slow stream (`tail -f | kqq`) sees rows promptly while bulk input is
        // written in large batches rather than one syscall per row.
        if (streaming_emit and reader.bufferedLen() == 0) try writer.flush();
        // Fast path: takeDelimiter returns a zero-copy slice if the line fits in the
        // reader buffer. For lines > 64 KB it returns StreamTooLong (consuming
        // exactly one buffer-worth of bytes); the slow path then keeps calling
        // takeDelimiter until it finds '\n', discarding the consumed prefix.
        const raw_line: []const u8 = blk: {
            if (reader.takeDelimiter('\n') catch |err| switch (err) {
                error.StreamTooLong => null,
                else => return err,
            }) |slice| {
                break :blk slice;
            }
            // Slow path: line >= 64 KB.  takeDelimiter on StreamTooLong does NOT
            // consume the buffer, so the first peek here returns those same bytes.
            // Accumulate chunks via peek+toss until '\n' or EOF.
            line_buf.clearRetainingCapacity();
            while (true) {
                const window: []const u8 = std.Io.Reader.peekGreedy(reader, 1) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                if (window.len == 0) break;
                if (std.mem.indexOfScalar(u8, window, '\n')) |pos| {
                    try line_buf.appendSlice(window[0..pos]);
                    std.Io.Reader.toss(reader, pos + 1);
                    break;
                }
                try line_buf.appendSlice(window);
                std.Io.Reader.toss(reader, window.len);
            }
            if (line_buf.items.len == 0) break :ndjson_loop;
            break :blk line_buf.items;
        };

        const trimmed = std.mem.trimEnd(u8, raw_line, " \t\r");
        if (trimmed.len == 0) continue :ndjson_loop;

        // --tee: write every line (match or not, record or not) to the tee file
        if (opts.tee_writer) |tw| {
            try tw.writeAll(trimmed);
            try tw.writeByte('\n');
        }

        // Skip lines that don't start an object (arrays, bare values, comments)
        if (trimmed[0] != '{') continue :ndjson_loop;

        // A JSON array written one element per line leaves a trailing comma.
        const line = if (trimmed[trimmed.len - 1] == ',')
            std.mem.trimEnd(u8, trimmed[0 .. trimmed.len - 1], " \t")
        else
            trimmed;

        // ── Raw-byte fast path ────────────────────────────────────────────────
        // Attempt WHERE evaluation directly on raw bytes before any parsing.
        // Lines emitted (or rejected) verbatim are validated first, so a
        // malformed line is skipped exactly as the parsing path would skip it.
        var where_passed = false;
        if (q.where) |w| {
            if (tryWhereOnRaw(line, w)) |passes| {
                if (!passes) {
                    if (opts.reject_writer) |rw| {
                        if (stream.isValidJson(line)) {
                            try rw.writeAll(line);
                            try rw.writeByte('\n');
                        } else if (opts.skipped_records) |sk| sk.* += 1;
                    }
                    continue :ndjson_loop;
                }
                // WHERE passed on raw bytes — for SELECT * in streaming mode,
                // emit the line directly without building a record.
                if (streaming_emit and sel == null) {
                    if (stream.isValidJson(line)) {
                        try emitter.emitLine(line);
                    } else if (opts.skipped_records) |sk| sk.* += 1;
                    continue :ndjson_loop;
                }
                where_passed = true; // still need projection / buffering
            }
            // null → raw evaluation inconclusive, fall through to full parse
        }

        _ = scratch.reset(.retain_capacity);
        var rec = parseLineRecord(scratch.allocator(), line) catch |err| {
            if (!stream.isSyntaxError(err)) return err;
            if (opts.skipped_records) |sk| sk.* += 1;
            continue :ndjson_loop;
        };

        if (q.where) |w| {
            if (!where_passed and !recordPassesWhere(scratch.allocator(), &rec, w)) {
                // --reject: write non-matching records to the reject file
                if (opts.reject_writer) |rw| {
                    try rw.writeAll(line);
                    try rw.writeByte('\n');
                }
                continue :ndjson_loop;
            }
        }

        if (streaming_emit) {
            if (sel) |fields| {
                // Project into scratch arena (all allocs freed on next scratch.reset())
                var out_rec = try projectRecord(scratch.allocator(), &rec, fields);
                try emitter.emitRecord(&out_rec);
            } else {
                // No field list (bare passthrough / WHERE only): emit the line itself
                try emitter.emitLine(line);
            }
            continue :ndjson_loop;
        }

        // ── Buffered path (ORDER BY / EXPAND / CSV) ──────────────────────────
        try matches.append(try copyRecordFull(allocator, &rec));
    }

    if (streaming_emit) {
        try emitter.finish();
        return emitter.count;
    }
    return finalizeMatches(allocator, matches.items, q, writer, opts);
}

/// Parse one NDJSON line holding exactly one object (anything after the
/// closing brace is a syntax error).
fn parseLineRecord(allocator: std.mem.Allocator, line: []const u8) !Record {
    var line_reader = std.Io.Reader.fixed(line);
    var sc = try stream.Scanner.init(allocator, &line_reader);
    if (try sc.next() != .object_start) return error.BadJson;
    const rec = try collectRecord(allocator, &sc);
    if (try sc.next() != .end_of_input) return error.BadJson;
    return rec;
}

// ─── Group-by + count executor ───────────────────────────────────────────────
//
// Handles queries like:
//   group by status select status, count() order by count desc limit 10
//   group by service select service, count() as total where level = "error"
//
// Works for both NDJSON and scoped (wrapped array) input.
// After WHERE filtering, groups records by the group_by field value,
// counting occurrences. Then projects the group key + count into result Records,
// sorts, limits, and emits.

// ─── Aggregate accumulators ──────────────────────────────────────────────────

/// The value an aggregate SELECT field reads from a record: its expression
/// when one was written (`sum(price * qty)`), otherwise the named field.
fn aggInput(scratch: std.mem.Allocator, rec: *const Record, f: SelectField) ?OwnedValue {
    if (f.expr) |e| return evalExpr(scratch, rec, e);
    return rec.get(f.key);
}

/// Running state for the aggregate fields of a SELECT list (sum, avg, min,
/// max, stddev, variance, count(x)) — one slot per aggregate, in SELECT order.
/// stddev/variance use Welford's online algorithm (means + m2s).
const AggAccum = struct {
    sums: []f64,
    mins: []f64,
    maxs: []f64,
    means: []f64,
    m2s: []f64,
    n_num: []usize, // numeric inputs seen, per slot
    n_nonnull: []usize, // non-null inputs seen, per slot (count(x))

    fn init(allocator: std.mem.Allocator, n: usize) !AggAccum {
        const floats = try allocator.alloc(f64, 5 * n);
        errdefer allocator.free(floats);
        const counts = try allocator.alloc(usize, 2 * n);
        @memset(floats[0 .. 2 * n], 0.0); // sums, means (below)
        const acc = AggAccum{
            .sums = floats[0..n],
            .means = floats[n .. 2 * n],
            .m2s = floats[2 * n .. 3 * n],
            .mins = floats[3 * n .. 4 * n],
            .maxs = floats[4 * n .. 5 * n],
            .n_num = counts[0..n],
            .n_nonnull = counts[n .. 2 * n],
        };
        @memset(acc.m2s, 0.0);
        @memset(acc.mins, std.math.floatMax(f64));
        @memset(acc.maxs, -std.math.floatMax(f64));
        @memset(counts, 0);
        return acc;
    }

    fn deinit(self: AggAccum, allocator: std.mem.Allocator) void {
        allocator.free(self.sums.ptr[0 .. 5 * self.sums.len]);
        allocator.free(self.n_num.ptr[0 .. 2 * self.n_num.len]);
    }

    /// Fold one input value into slot `i`. Null and missing values are ignored
    /// (SQL semantics); non-numeric values only count towards count(x).
    fn add(self: AggAccum, i: usize, v: ?OwnedValue) void {
        const val = v orelse return;
        if (val == .null_val) return;
        self.n_nonnull[i] += 1;
        const n = toF64(val) orelse return;
        self.sums[i] += n;
        if (n < self.mins[i]) self.mins[i] = n;
        if (n > self.maxs[i]) self.maxs[i] = n;
        self.n_num[i] += 1;
        const cnt: f64 = @floatFromInt(self.n_num[i]);
        const delta = n - self.means[i];
        self.means[i] += delta / cnt;
        self.m2s[i] += delta * (n - self.means[i]);
    }

    /// Feed one record to every aggregate field of `fields`.
    fn addRecord(self: AggAccum, scratch: std.mem.Allocator, rec: *const Record, fields: []const SelectField) void {
        var i: usize = 0;
        for (fields) |f| {
            const agg = f.agg orelse continue;
            if (agg == .count_rows) {
                self.n_nonnull[i] += 1;
            } else {
                self.add(i, aggInput(scratch, rec, f));
            }
            i += 1;
        }
    }

    fn result(self: AggAccum, i: usize, agg: AggFunc) OwnedValue {
        if (agg == .count or agg == .count_rows) return OwnedValue.num(@floatFromInt(self.n_nonnull[i]));
        const cnt = self.n_num[i];
        if (cnt == 0) return .null_val;
        const n: f64 = @floatFromInt(cnt);
        return OwnedValue.num(switch (agg) {
            .sum => self.sums[i],
            .avg => self.sums[i] / n,
            .min => self.mins[i],
            .max => self.maxs[i],
            .variance => self.m2s[i] / n,
            .stddev => @sqrt(self.m2s[i] / n),
            .count, .count_rows => unreachable,
        });
    }

    /// Append every aggregate column to `rec`, in SELECT order.
    fn appendResults(
        self: AggAccum,
        allocator: std.mem.Allocator,
        rec: *Record,
        fields: ?[]const SelectField,
    ) !void {
        const flds = fields orelse return;
        var i: usize = 0;
        for (flds) |f| {
            const agg = f.agg orelse continue;
            const name = try allocator.dupe(u8, if (f.alias) |a| a else f.key);
            errdefer allocator.free(name);
            try rec.fields.put(allocator, name, self.result(i, agg));
            i += 1;
        }
    }
};

fn countAggFields(fields: ?[]const SelectField) usize {
    var n: usize = 0;
    if (fields) |flds| for (flds) |f| {
        if (f.agg != null) n += 1;
    };
    return n;
}

/// Global (no GROUP BY) aggregation: `select count(), avg(x) where ...`.
const GlobalAgg = struct {
    accum: AggAccum,
    q: *const query.Query,

    fn init(allocator: std.mem.Allocator, q: *const query.Query) !GlobalAgg {
        return .{ .accum = try AggAccum.init(allocator, countAggFields(q.fields)), .q = q };
    }

    fn deinit(self: *GlobalAgg, allocator: std.mem.Allocator) void {
        self.accum.deinit(allocator);
    }

    /// Apply WHERE and fold the record in. Returns whether it was counted.
    fn add(self: *GlobalAgg, scratch: std.mem.Allocator, rec: *const Record) bool {
        if (self.q.where) |w| {
            if (!recordPassesWhere(scratch, rec, w)) return false;
        }
        if (self.q.fields) |f| self.accum.addRecord(scratch, rec, f);
        return true;
    }

    fn toRecord(self: *const GlobalAgg, allocator: std.mem.Allocator) !Record {
        var rec = Record.init(allocator);
        errdefer rec.deinit();
        try self.accum.appendResults(allocator, &rec, self.q.fields);
        return rec;
    }

    /// --rolling: one NDJSON line with the aggregates so far.
    fn emitSnapshot(self: *const GlobalAgg, allocator: std.mem.Allocator, writer: *std.Io.Writer) !void {
        var rec = try self.toRecord(allocator);
        defer rec.deinit();
        try writeRecordJson(writer, &rec);
        try writer.writeByte('\n');
        try writer.flush();
    }

    fn emit(self: *const GlobalAgg, allocator: std.mem.Allocator, writer: *std.Io.Writer, opts: ExecOptions) !usize {
        var rec = try self.toRecord(allocator);
        defer rec.deinit();
        var one = [1]Record{rec};
        try emitResults(&one, 1, writer, opts.format, opts.raw);
        return 1;
    }
};

/// Append a type-tagged, length-prefixed encoding of `v` to `buf`, so that
/// distinct values never collide (1 vs "1", or strings containing separators).
/// Used for GROUP BY keys and DISTINCT fingerprints.
fn appendValueKey(buf: *std.array_list.Managed(u8), v: OwnedValue) !void {
    var tmp: [64]u8 = undefined;
    const tag: u8, const bytes: []const u8 = switch (v) {
        .string => |sv| .{ 's', sv },
        // -0 and 0 are the same value
        .number => |n| .{ 'n', if (n.lit == null and n.f == 0) "0" else n.text(&tmp) },
        .boolean => |b| .{ 'b', if (b) "1" else "0" },
        .null_val => .{ 'z', "" },
        .raw => |r| .{ 'r', r },
    };
    try buf.append(tag);
    var len: [4]u8 = undefined;
    std.mem.writeInt(u32, &len, @intCast(bytes.len), .little);
    try buf.appendSlice(&len);
    try buf.appendSlice(bytes);
}

/// Internal group-by aggregation over records produced by `records_iter`.
/// Returns a new list of Records (one per group), owned by `allocator`.
fn aggregateGroupBy(
    allocator: std.mem.Allocator,
    records_iter: anytype, // must support: while(try iter.next()) |rec| { ... }
    group_keys: []const query.GroupByKey,
    extra_fields: ?[]const SelectField, // the SELECT list (aggregates are read from it)
    where: ?query.WhereClause,
) !std.array_list.Managed(Record) {
    const n_group_keys = group_keys.len;
    if (n_group_keys > 16) return error.TooManyGroupKeys;
    // Per-group state
    const AggState = struct {
        group_vals: []OwnedValue, // one per group key, heap-owned slice
        accum: AggAccum,
    };

    const n_aggs = countAggFields(extra_fields);

    // isnull(key, default) in SELECT supplies a value for records whose group
    // key is missing or null. Resolved once per key (by output name).
    var isnull_defaults: [16]?[]const u8 = @splat(null);
    for (group_keys, 0..) |gk, gi| {
        for (extra_fields orelse break) |f| {
            const matches = std.mem.eql(u8, f.key, gk.key) or
                (f.alias != null and std.mem.eql(u8, f.alias.?, gk.key));
            if (matches and f.default_val != null) {
                isnull_defaults[gi] = f.default_val;
                break;
            }
        }
    }

    var states = std.StringHashMap(AggState).init(allocator);
    defer {
        var sit = states.iterator();
        while (sit.next()) |e| {
            allocator.free(e.key_ptr.*);
            for (e.value_ptr.group_vals) |*gv| gv.deinit(allocator);
            allocator.free(e.value_ptr.group_vals);
            e.value_ptr.accum.deinit(allocator);
        }
        states.deinit();
    }

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Iterate records via the provided iterator
    while (try records_iter.next()) |rec| {
        var r = rec;
        defer _ = scratch.reset(.retain_capacity);

        if (where) |w| {
            if (!recordPassesWhere(scratch.allocator(), &r, w)) continue;
        }

        // Build a composite group key from all group-by values.
        // If any key field is missing (and no isnull default), skip the record.
        var composite_buf = std.array_list.Managed(u8).init(scratch.allocator());
        var gv_list: [16]OwnedValue = undefined; // stack buffer, supports up to 16 group keys
        var skip_record = false;

        for (group_keys, 0..) |gk, gi| {
            // Evaluate: if the group key has an expression, eval it; otherwise field lookup.
            const gv_raw: ?OwnedValue = if (gk.expr) |expr|
                evalExpr(scratch.allocator(), &r, expr)
            else
                r.get(gk.key);
            const isnull_default = isnull_defaults[gi];

            const gv: OwnedValue = if (gv_raw) |v| blk: {
                if (v == .null_val) {
                    if (isnull_default) |def| break :blk OwnedValue{ .string = @constCast(def) };
                }
                break :blk v;
            } else if (isnull_default) |def|
                OwnedValue{ .string = @constCast(def) }
            else {
                skip_record = true;
                break;
            };

            try appendValueKey(&composite_buf, gv);
            gv_list[gi] = gv;
        }

        if (skip_record) continue;

        const key_str = composite_buf.items;
        const gop = try states.getOrPut(key_str);
        if (!gop.found_existing) {
            errdefer _ = states.remove(key_str);
            const owned_key = try allocator.dupe(u8, key_str);
            errdefer allocator.free(owned_key);
            // Own copies of each group value
            const owned_gvs = try allocator.alloc(OwnedValue, n_group_keys);
            errdefer allocator.free(owned_gvs);
            for (gv_list[0..n_group_keys], 0..) |gv, gi| {
                owned_gvs[gi] = try gv.copy(allocator);
            }
            const accum = try AggAccum.init(allocator, n_aggs);
            gop.key_ptr.* = owned_key;
            gop.value_ptr.* = .{ .group_vals = owned_gvs, .accum = accum };
        }
        if (extra_fields) |ef| gop.value_ptr.accum.addRecord(scratch.allocator(), &r, ef);
    }

    // Build result Records
    var result = std.array_list.Managed(Record).init(allocator);
    errdefer {
        for (result.items) |*ri| ri.deinit();
        result.deinit();
    }

    var it = states.iterator();
    while (it.next()) |e| {
        const state = e.value_ptr;
        var rec = Record.init(allocator);
        errdefer rec.deinit();

        // Add all group-by key fields
        for (group_keys, 0..) |gk, gi| {
            const gk_name = try allocator.dupe(u8, gk.key);
            errdefer allocator.free(gk_name);
            const gv_copy = try state.group_vals[gi].copy(allocator);
            try rec.fields.put(allocator, gk_name, gv_copy);
        }

        // The aggregate columns (count() included), in SELECT order
        try state.accum.appendResults(allocator, &rec, extra_fields);

        try result.append(rec);
    }

    return result;
}

/// Iterator over the objects of a newline-delimited JSON stream (objects may
/// also span lines, or sit inside a top-level array). A malformed or
/// truncated object is skipped — counted in `skipped`, like the line-based
/// executor does — and reading resumes with the next line.
const NdjsonIter = struct {
    sc: *stream.Scanner,
    scratch_arena: *std.heap.ArenaAllocator,
    skipped: ?*usize = null,

    pub fn next(self: *NdjsonIter) !?Record {
        while (true) {
            var in_record = false;
            const rec = self.nextObject(&in_record) catch |err| {
                if (!stream.isSyntaxError(err)) return err;
                // Garbage between records is ignored silently, matching the
                // line-based path, which skips lines that do not start with '{'.
                if (in_record) {
                    if (self.skipped) |sk| sk.* += 1;
                }
                try self.sc.recoverAtLine();
                continue;
            };
            return rec;
        }
    }

    fn nextObject(self: *NdjsonIter, in_record: *bool) !?Record {
        while (true) {
            in_record.* = self.sc.depth() > 0;
            switch (try self.sc.next()) {
                .end_of_input => return null,
                .object_start => {
                    in_record.* = true;
                    _ = self.scratch_arena.reset(.retain_capacity);
                    const rec = try collectRecord(self.scratch_arena.allocator(), self.sc);
                    // A top-level record must end its line, as in the line-based path.
                    if (self.sc.depth() == 0 and !try self.sc.atLineEnd()) return error.BadJson;
                    return rec;
                },
                // Top-level scalars and array brackets are not records.
                else => {},
            }
        }
    }
};

/// Iterator wrapper around a Scanner reading a scoped JSON array
const ScopedIter = struct {
    sc: *stream.Scanner,
    scratch_arena: *std.heap.ArenaAllocator,
    done: bool = false,

    pub fn next(self: *ScopedIter) !?Record {
        if (self.done) return null;
        while (true) {
            const ev = try self.sc.next();
            switch (ev) {
                .array_end, .end_of_input => {
                    self.done = true;
                    return null;
                },
                .object_start => {
                    _ = self.scratch_arena.reset(.retain_capacity);
                    const rec = try collectRecord(self.scratch_arena.allocator(), self.sc);
                    return rec;
                },
                else => {},
            }
        }
    }
};

/// Execute a group-by query on NDJSON input.
pub fn execGroupByNDJSON(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.group_by == null) return error.NoGroupByKey;

    var sc = try stream.Scanner.init(allocator, reader);
    defer sc.deinit();
    sc.allow_top_level_commas = true;
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var iter = NdjsonIter{ .sc = &sc, .scratch_arena = &scratch, .skipped = opts.skipped_records };
    var groups = try aggregateGroupBy(allocator, &iter, q.group_by.?, q.fields, q.where);
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }
    return finishGroups(allocator, &groups, q, writer, opts);
}

/// HAVING → ORDER BY → LIMIT → emit, for the per-group result records.
fn finishGroups(
    allocator: std.mem.Allocator,
    groups: *std.array_list.Managed(Record),
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    // HAVING: filter group results
    if (q.having) |h| {
        var scratch_h = std.heap.ArenaAllocator.init(allocator);
        defer scratch_h.deinit();
        var i: usize = 0;
        while (i < groups.items.len) {
            if (recordPassesWhere(scratch_h.allocator(), &groups.items[i], h)) {
                i += 1;
            } else {
                var removed = groups.orderedRemove(i);
                removed.deinit();
            }
        }
    }

    if (q.order_by) |order| sortRecords(groups.items, order);

    const lim = q.limit orelse std.math.maxInt(usize);
    const count = @min(groups.items.len, lim);
    try emitResults(groups.items, count, writer, opts.format, opts.raw);
    return count;
}

/// Stable sort by ORDER BY keys.
fn sortRecords(records: []Record, order: []const OrderField) void {
    const Ctx = struct {
        ob: []const OrderField,
        pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
            return compareRecords(ctx.ob, &a, &b) == .lt;
        }
    };
    std.sort.block(Record, records, Ctx{ .ob = order }, Ctx.lessThan);
}

/// Execute a group-by query on scoped (wrapped array) JSON input.
pub fn execGroupByStream(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.group_by == null) return error.NoGroupByKey;

    var sc = try stream.Scanner.init(allocator, reader);
    defer sc.deinit();

    const scope = q.scope_pattern orelse return error.NoScopeInStreamMode;
    if (std.mem.eql(u8, scope, "*")) return error.AutoScopeNotSupportedInStreamMode;

    const star_pos = std.mem.lastIndexOfScalar(u8, scope, '*') orelse scope.len;
    const nav_path = if (star_pos >= 2 and scope[star_pos - 1] == '.')
        scope[0 .. star_pos - 1]
    else
        scope;
    const found = try navigateToArray(&sc, nav_path);
    if (!found) {
        try writer.writeAll("[]\n");
        return 0;
    }

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var iter = ScopedIter{ .sc = &sc, .scratch_arena = &scratch };
    var groups = try aggregateGroupBy(allocator, &iter, q.group_by.?, q.fields, q.where);
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }
    return finishGroups(allocator, &groups, q, writer, opts);
}

// ─── Global aggregate executor ───────────────────────────────────────────────

pub fn execGlobalAggNDJSON(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.fields == null) return 0;
    var sc = try stream.Scanner.init(allocator, reader);
    defer sc.deinit();
    sc.allow_top_level_commas = true;
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var agg = try GlobalAgg.init(allocator, &q);
    defer agg.deinit(allocator);
    var iter = NdjsonIter{ .sc = &sc, .scratch_arena = &scratch, .skipped = opts.skipped_records };
    while (try iter.next()) |rec| _ = agg.add(scratch.allocator(), &rec);
    return agg.emit(allocator, writer, opts);
}

// ─── LLM stream mode ────────────────────────────────────────────────────────
//
// Reads an NDJSON stream of LLM envelope objects (e.g. from Ollama), extracts
// token fragments from a named field (e.g. "response"), accumulates them into
// a buffer, and detects complete JSON objects using brace/bracket depth tracking.
//
// Each time a top-level object completes inside the accumulated text, it is
// parsed, run through the query (WHERE → SELECT), and emitted immediately.
// This enables real-time querying of LLM-generated JSON as it streams in.
//
// Usage:
//   curl ... | kqq --llm response 'select make, model where type = "sedan"'

pub fn execLlmStream(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const use_sse = opts.api_mode != null and opts.api_mode.? != .ollama;
    const sel = if (q.fields) |f| @as(?[]const SelectField, f) else null;
    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    const early_stop = canStopEarly(q);
    const lim = q.limit orelse std.math.maxInt(usize);
    const fmt = if (opts.raw) OutputFormat.raw else opts.format;

    // When no ORDER BY and no DISTINCT, emit each matched record immediately
    // to the writer as it arrives rather than buffering for post-processing.
    const streaming_emit = ob == null and !q.distinct and expandField(sel) == null and
        fmt != .csv and fmt != .tsv;
    var emit_count: usize = 0;

    // Accumulator for the LLM token fragments
    var accum = std.array_list.Managed(u8).init(allocator);
    defer accum.deinit();

    // Collected output records (for ORDER BY / post-processing)
    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Line-reading buffer for NDJSON envelopes
    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();

    // Read envelope lines (one per token from the LLM API)
    envelope_loop: while (true) {
        const raw_line: []const u8 = blk: {
            if (reader.takeDelimiter('\n') catch |err| switch (err) {
                error.StreamTooLong => null,
                else => return err,
            }) |slice| {
                break :blk slice;
            }
            line_buf.clearRetainingCapacity();
            while (true) {
                const window: []const u8 = std.Io.Reader.peekGreedy(reader, 1) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                if (window.len == 0) break;
                if (std.mem.indexOfScalar(u8, window, '\n')) |pos| {
                    try line_buf.appendSlice(window[0..pos]);
                    std.Io.Reader.toss(reader, pos + 1);
                    break;
                }
                try line_buf.appendSlice(window);
                std.Io.Reader.toss(reader, window.len);
            }
            if (line_buf.items.len == 0) break :envelope_loop;
            break :blk line_buf.items;
        };

        const trimmed = std.mem.trimEnd(u8, raw_line, " \t\r");
        if (trimmed.len == 0) continue;

        // --tee: write every raw envelope line
        if (opts.tee_writer) |tw| {
            try tw.writeAll(trimmed);
            try tw.writeByte('\n');
        }

        // SSE framing: strip "data: " prefix, skip comments/[DONE]
        const json_line = if (use_sse)
            (stripSseLine(trimmed) orelse continue)
        else
            trimmed;

        if (json_line.len == 0 or json_line[0] != '{') continue;

        // Extract the token fragment using either flat field or nested path
        _ = scratch.reset(.retain_capacity);
        const fragment: []const u8 = if (opts.llm_path) |path| blk: {
            // Nested path extraction (e.g. choices.0.delta.content)
            break :blk extractNestedField(scratch.allocator(), json_line, path) orelse continue;
        } else blk: {
            // Flat field extraction via streaming parser
            const llm_field = opts.llm_field orelse return error.NoLlmField;
            var env_reader = std.Io.Reader.fixed(json_line);
            var env_sc = stream.Scanner.init(scratch.allocator(), &env_reader) catch continue;
            const first_ev = env_sc.next() catch continue;
            if (first_ev != .object_start) continue;

            var env_rec = collectRecord(scratch.allocator(), &env_sc) catch continue;
            const fragment_ov = env_rec.get(llm_field) orelse continue;
            break :blk switch (fragment_ov) {
                .string => |s| s,
                else => continue,
            };
        };

        if (fragment.len == 0) continue;

        // Append fragment to accumulator
        try accum.appendSlice(fragment);

        // Try to extract complete JSON objects from the accumulator.
        // Strategy: scan for brace/bracket depth, handling quoted strings.
        while (extractNextObject(accum.items)) |obj_span| {
            const obj_bytes = accum.items[obj_span.start..obj_span.end];

            // Parse the extracted object and run the query against it
            _ = scratch.reset(.retain_capacity);
            var obj_reader = std.Io.Reader.fixed(obj_bytes);
            var obj_sc = stream.Scanner.init(scratch.allocator(), &obj_reader) catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };
            const obj_ev = obj_sc.next() catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };
            if (obj_ev != .object_start) {
                drainAccum(&accum, obj_span.end);
                continue;
            }

            var rec = collectRecord(scratch.allocator(), &obj_sc) catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };

            // Drain the consumed bytes from the accumulator
            drainAccum(&accum, obj_span.end);

            // Schema validation (--expect)
            if (opts.expect_schema) |schema| {
                if (!validateSchema(&rec, schema)) {
                    if (opts.reject_writer) |rw| {
                        try rw.writeAll(obj_bytes);
                        try rw.writeByte('\n');
                    }
                    continue;
                }
            }

            // WHERE filter
            if (q.where) |w| {
                if (!recordPassesWhere(scratch.allocator(), &rec, w)) {
                    // --reject: write serialized object
                    if (opts.reject_writer) |rw| {
                        try rw.writeAll(obj_bytes);
                        try rw.writeByte('\n');
                    }
                    continue;
                }
            }

            const full_rec = try copyRecordFull(allocator, &rec);
            if (streaming_emit) {
                defer {
                    var tmp = full_rec;
                    tmp.deinit();
                }
                var out_rec: Record = if (sel) |s|
                    try projectRecord(allocator, &full_rec, s)
                else
                    try copyRecordFull(allocator, &full_rec);
                defer out_rec.deinit();

                switch (fmt) {
                    .json => {
                        if (emit_count == 0) try writer.writeAll("[\n");
                        if (emit_count > 0) try writer.writeAll(",\n");
                        try writer.writeAll("  ");
                        try writeRecordJson(writer, &out_rec);
                    },
                    .ndjson => {
                        try writeRecordJson(writer, &out_rec);
                        try writer.writeByte('\n');
                    },
                    .raw => {
                        var it = out_rec.fields.iterator();
                        if (it.next()) |e| {
                            try writeOwnedValueRaw(writer, e.value_ptr.*);
                            try writer.writeByte('\n');
                        }
                    },
                    .csv, .tsv => unreachable, // streaming_emit is false for csv/tsv
                }
                try writer.flush();
                emit_count += 1;
                if (early_stop and emit_count >= lim) break :envelope_loop;
            } else {
                try matches.append(full_rec);
                if (early_stop and matches.items.len >= lim) break :envelope_loop;
            }
        }
    }

    // Warn if the stream ended with unconsumed bytes in the accumulator
    // (indicates the LLM response was truncated / closed mid-object).
    if (accum.items.len > 0) {
        std.debug.print("kqq: warning: stream ended with {d} bytes of incomplete JSON discarded\n", .{accum.items.len});
    }

    // Streaming path: close the JSON array and return without further buffering.
    if (streaming_emit) {
        switch (fmt) {
            .json => if (emit_count == 0)
                try writer.writeAll("[]\n")
            else
                try writer.writeAll("\n]\n"),
            .ndjson, .raw => {}, // already written line-by-line, nothing to close
            .csv, .tsv => unreachable,
        }
        return emit_count;
    }

    return finalizeMatches(allocator, matches.items, q, writer, opts);
}

pub fn execLlmGlobalAgg(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const use_sse = opts.api_mode != null and opts.api_mode.? != .ollama;
    if (q.fields == null) return 0;
    var agg = try GlobalAgg.init(allocator, &q);
    defer agg.deinit(allocator);

    var accum = std.array_list.Managed(u8).init(allocator);
    defer accum.deinit();

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();

    // Read envelope lines
    envelope_loop: while (true) {
        const raw_line: []const u8 = blk: {
            if (reader.takeDelimiter('\n') catch |err| switch (err) {
                error.StreamTooLong => null,
                else => return err,
            }) |slice| {
                break :blk slice;
            }
            line_buf.clearRetainingCapacity();
            while (true) {
                const window: []const u8 = std.Io.Reader.peekGreedy(reader, 1) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                if (window.len == 0) break;
                if (std.mem.indexOfScalar(u8, window, '\n')) |pos| {
                    try line_buf.appendSlice(window[0..pos]);
                    std.Io.Reader.toss(reader, pos + 1);
                    break;
                }
                try line_buf.appendSlice(window);
                std.Io.Reader.toss(reader, window.len);
            }
            if (line_buf.items.len == 0) break :envelope_loop;
            break :blk line_buf.items;
        };

        const trimmed = std.mem.trimEnd(u8, raw_line, " \t\r");
        if (trimmed.len == 0) continue;

        // SSE framing
        const json_line = if (use_sse)
            (stripSseLine(trimmed) orelse continue)
        else
            trimmed;

        if (json_line.len == 0 or json_line[0] != '{') continue;

        _ = scratch.reset(.retain_capacity);
        const fragment: []const u8 = if (opts.llm_path) |path| blk: {
            break :blk extractNestedField(scratch.allocator(), json_line, path) orelse continue;
        } else blk: {
            const llm_field = opts.llm_field orelse return error.NoLlmField;
            var env_reader = std.Io.Reader.fixed(json_line);
            var env_sc = stream.Scanner.init(scratch.allocator(), &env_reader) catch continue;
            const first_ev = env_sc.next() catch continue;
            if (first_ev != .object_start) continue;
            var env_rec = collectRecord(scratch.allocator(), &env_sc) catch continue;
            const fragment_ov = env_rec.get(llm_field) orelse continue;
            break :blk switch (fragment_ov) {
                .string => |s| s,
                else => continue,
            };
        };
        if (fragment.len == 0) continue;
        try accum.appendSlice(fragment);

        while (extractNextObject(accum.items)) |obj_span| {
            const obj_bytes = accum.items[obj_span.start..obj_span.end];

            _ = scratch.reset(.retain_capacity);
            var obj_reader = std.Io.Reader.fixed(obj_bytes);
            var obj_sc = stream.Scanner.init(scratch.allocator(), &obj_reader) catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };
            const obj_ev = obj_sc.next() catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };
            if (obj_ev != .object_start) {
                drainAccum(&accum, obj_span.end);
                continue;
            }

            var rec = collectRecord(scratch.allocator(), &obj_sc) catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };

            drainAccum(&accum, obj_span.end);

            if (!agg.add(scratch.allocator(), &rec)) continue;

            // --rolling: emit a snapshot of the running aggregates after each object
            if (opts.rolling) try agg.emitSnapshot(allocator, writer);
        }
    }

    if (opts.rolling) return 1;
    return agg.emit(allocator, writer, opts);
}

/// LLM group-by mode — accumulates token fragments, extracts objects, aggregates by group.
pub fn execLlmGroupBy(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const use_sse = opts.api_mode != null and opts.api_mode.? != .ollama;
    if (q.group_by == null) return error.NoGroupByKey;

    var accum = std.array_list.Managed(u8).init(allocator);
    defer accum.deinit();

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();

    // Collect records from the LLM stream into a list, then use aggregateGroupBy
    var collected = std.array_list.Managed(Record).init(allocator);
    defer {
        for (collected.items) |*r| r.deinit();
        collected.deinit();
    }

    envelope_loop: while (true) {
        const raw_line: []const u8 = blk: {
            if (reader.takeDelimiter('\n') catch |err| switch (err) {
                error.StreamTooLong => null,
                else => return err,
            }) |slice| {
                break :blk slice;
            }
            line_buf.clearRetainingCapacity();
            while (true) {
                const window: []const u8 = std.Io.Reader.peekGreedy(reader, 1) catch |err| switch (err) {
                    error.EndOfStream => break,
                    else => return err,
                };
                if (window.len == 0) break;
                if (std.mem.indexOfScalar(u8, window, '\n')) |pos| {
                    try line_buf.appendSlice(window[0..pos]);
                    std.Io.Reader.toss(reader, pos + 1);
                    break;
                }
                try line_buf.appendSlice(window);
                std.Io.Reader.toss(reader, window.len);
            }
            if (line_buf.items.len == 0) break :envelope_loop;
            break :blk line_buf.items;
        };

        const trimmed = std.mem.trimEnd(u8, raw_line, " \t\r");
        if (trimmed.len == 0) continue;

        // SSE framing
        const json_line = if (use_sse)
            (stripSseLine(trimmed) orelse continue)
        else
            trimmed;

        if (json_line.len == 0 or json_line[0] != '{') continue;

        _ = scratch.reset(.retain_capacity);
        const fragment: []const u8 = if (opts.llm_path) |path| blk: {
            break :blk extractNestedField(scratch.allocator(), json_line, path) orelse continue;
        } else blk: {
            const llm_field = opts.llm_field orelse return error.NoLlmField;
            var env_reader = std.Io.Reader.fixed(json_line);
            var env_sc = stream.Scanner.init(scratch.allocator(), &env_reader) catch continue;
            const first_ev = env_sc.next() catch continue;
            if (first_ev != .object_start) continue;
            var env_rec = collectRecord(scratch.allocator(), &env_sc) catch continue;
            const fragment_ov = env_rec.get(llm_field) orelse continue;
            break :blk switch (fragment_ov) {
                .string => |s| s,
                else => continue,
            };
        };
        if (fragment.len == 0) continue;
        try accum.appendSlice(fragment);

        while (extractNextObject(accum.items)) |obj_span| {
            const obj_bytes = accum.items[obj_span.start..obj_span.end];

            _ = scratch.reset(.retain_capacity);
            var obj_reader = std.Io.Reader.fixed(obj_bytes);
            var obj_sc = stream.Scanner.init(scratch.allocator(), &obj_reader) catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };
            const obj_ev = obj_sc.next() catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };
            if (obj_ev != .object_start) {
                drainAccum(&accum, obj_span.end);
                continue;
            }

            var rec = collectRecord(scratch.allocator(), &obj_sc) catch {
                drainAccum(&accum, obj_span.end);
                continue;
            };

            drainAccum(&accum, obj_span.end);

            if (q.where) |w| {
                if (!recordPassesWhere(scratch.allocator(), &rec, w)) continue;
            }

            const full_rec = try copyRecordFull(allocator, &rec);
            try collected.append(full_rec);

            // --rolling: re-aggregate and emit snapshot after each new object
            if (opts.rolling) {
                var snap_iter = SliceIter{ .records = collected.items, .idx = 0 };
                var snap_groups = try aggregateGroupBy(allocator, &snap_iter, q.group_by.?, q.fields, null);
                defer {
                    for (snap_groups.items) |*sr| sr.deinit();
                    snap_groups.deinit();
                }
                try emitGroupBySnapshot(writer, snap_groups.items);
                try writer.flush();
            }
        }
    }

    // Now aggregate using the collected records via a slice iterator.
    // Pass null for where — records are already pre-filtered above.
    // In rolling mode, the final result was already emitted incrementally,
    // but we still emit the final summary for consistency.
    var iter = SliceIter{ .records = collected.items, .idx = 0 };
    var groups = try aggregateGroupBy(allocator, &iter, q.group_by.?, q.fields, null);
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }

    if (opts.rolling) return groups.items.len;
    return finishGroups(allocator, &groups, q, writer, opts);
}

/// Simple slice-based record iterator for feeding pre-collected records into aggregateGroupBy.
const SliceIter = struct {
    records: []Record,
    idx: usize,

    /// Returns Record by value (same interface as NdjsonIter / ScopedIter).
    fn next(self: *SliceIter) !?Record {
        if (self.idx >= self.records.len) return null;
        const rec = self.records[self.idx];
        self.idx += 1;
        return rec;
    }
};

/// Wraps any record source and deinits the *previous* record each time
/// `.next()` is called. This is needed because `aggregateGroupBy` does not
/// own the records — it reads fields and copies what it needs into AggState.
/// Arena-backed sources (NdjsonIter/ScopedIter) reset their arena inside
/// `.next()`, so records are implicitly freed. Heap-backed sources
/// (TextLineSource/DelimitedSource) need explicit deinit.
fn DeinitIter(comptime Inner: type) type {
    return struct {
        inner: *Inner,
        prev: ?Record,

        pub fn next(self: *@This()) !?Record {
            // Free the previous record (aggregateGroupBy is done with it)
            if (self.prev) |*p| p.deinit();
            self.prev = null;

            const rec = try self.inner.next() orelse return null;
            self.prev = rec;
            return rec;
        }
    };
}

// ─── Generic executors ───────────────────────────────────────────────────────
//
// These work with ANY record source (JSON, text, delimited, LLM accumulator)
// via duck-typed `anytype` — the source just needs `.next() !?Record`.
//
// This eliminates the duplication across the 9 format-specific executors.

/// Generic SELECT / WHERE / ORDER BY / LIMIT / DISTINCT executor.
/// Works with any record source that has `.next() !?Record`.
pub fn execGenericSource(
    allocator: std.mem.Allocator,
    source: anytype,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const early_stop = canStopEarly(q);
    const lim = q.limit orelse std.math.maxInt(usize);

    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Consume records from the source
    while (!(early_stop and matches.items.len >= lim)) {
        const rec_val = try source.next() orelse break;
        _ = scratch.reset(.retain_capacity);
        var rec = rec_val;

        // WHERE filter
        if (q.where) |w| {
            if (!recordPassesWhere(scratch.allocator(), &rec, w)) {
                rec.deinit();
                continue;
            }
        }

        // Copy into long-lived allocator
        const full_rec = try copyRecordFull(allocator, &rec);
        rec.deinit();
        try matches.append(full_rec);
    }

    return finalizeMatches(allocator, matches.items, q, writer, opts);
}

/// Generic GROUP BY executor. Works with any record source.
pub fn execGenericGroupBy(
    allocator: std.mem.Allocator,
    source: anytype,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.group_by == null) return error.NoGroupByKey;

    // Wrap source in a deiniting iterator that frees each record after the
    // next one is requested. aggregateGroupBy was designed for arena-backed
    // iterators (NdjsonIter/ScopedIter) where records are bulk-freed by the
    // arena reset. Generic sources allocate records on the heap, so we must
    // deinit them after aggregateGroupBy has extracted the needed values.
    var deinit_iter = DeinitIter(@TypeOf(source.*)){
        .inner = source,
        .prev = null,
    };
    var groups = try aggregateGroupBy(allocator, &deinit_iter, q.group_by.?, q.fields, q.where);
    // Deinit the last record if any
    if (deinit_iter.prev) |*p| p.deinit();
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }

    return finishGroups(allocator, &groups, q, writer, opts);
}

/// Generic global aggregate executor. Works with any record source.
pub fn execGenericGlobalAgg(
    allocator: std.mem.Allocator,
    source: anytype,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.fields == null) return 0;
    var agg = try GlobalAgg.init(allocator, &q);
    defer agg.deinit(allocator);

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    while (try source.next()) |rec_val| {
        _ = scratch.reset(.retain_capacity);
        var rec = rec_val;
        defer rec.deinit();
        if (!agg.add(scratch.allocator(), &rec)) continue;
        if (opts.rolling) try agg.emitSnapshot(allocator, writer);
    }

    if (opts.rolling) return 1;
    return agg.emit(allocator, writer, opts);
}

pub fn execGlobalAggStream(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    var sc = try stream.Scanner.init(allocator, reader);
    defer sc.deinit();
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    const scope = q.scope_pattern orelse return error.NoScope;
    const star_pos = std.mem.lastIndexOfScalar(u8, scope, '*') orelse scope.len;
    const nav_path = if (star_pos >= 2 and scope[star_pos - 1] == '.')
        scope[0 .. star_pos - 1]
    else
        scope[0..star_pos];
    const found = try navigateToArray(&sc, nav_path);
    if (!found) return 0;

    if (q.fields == null) return 0;
    var agg = try GlobalAgg.init(allocator, &q);
    defer agg.deinit(allocator);
    var iter = ScopedIter{ .sc = &sc, .scratch_arena = &scratch };
    while (try iter.next()) |rec| _ = agg.add(scratch.allocator(), &rec);
    return agg.emit(allocator, writer, opts);
}

// ─── TESTS ───────────────────────────────────────────────────────────────────

test "stream exec: filter and select" {
    const src =
        \\{"users":[
        \\  {"name":"Alice","score":98,"active":true},
        \\  {"name":"Bob","score":45,"active":false},
        \\  {"name":"Eve","score":91,"active":true}
        \\]}
    ;
    var reader1 = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "[users.*] select name, score where active = true order by score desc", null);
    defer q.deinit(allocator);

    // For testing, collect into buffer via a helper
    var result = std.array_list.Managed(u8).init(allocator);
    defer result.deinit();

    var sc = try stream.Scanner.init(allocator, &reader1);
    defer sc.deinit();

    const scope = q.scope_pattern.?;
    const star_pos = std.mem.lastIndexOfScalar(u8, scope, '*') orelse scope.len;
    const nav_path = if (star_pos >= 2 and scope[star_pos - 1] == '.')
        scope[0 .. star_pos - 1]
    else
        scope;

    _ = try navigateToArray(&sc, nav_path);

    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    while (true) {
        const ev = try sc.next();
        if (ev == .array_end or ev == .end_of_input) break;
        if (ev != .object_start) continue;
        var rec = try collectRecord(allocator, &sc);
        defer rec.deinit();
        if (q.where) |w| {
            if (!recordPassesWhere(allocator, &rec, w)) continue;
        }
        const out_rec = if (q.fields) |s| try projectRecord(allocator, &rec, s) else try copyRecord(allocator, &rec);
        try matches.append(out_rec);
    }

    // Sort
    if (q.order_by) |ob| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, matches.items, Ctx{ .ob = ob }, Ctx.lessThan);
    }

    try std.testing.expectEqual(@as(usize, 2), matches.items.len);
    // First should be Alice (score 98), second Eve (score 91) because desc
    try std.testing.expectEqualStrings("Alice", matches.items[0].get("name").?.string);
    try std.testing.expectEqualStrings("Eve", matches.items[1].get("name").?.string);
    // Score should be projected
    try std.testing.expect(matches.items[0].get("score") != null);
    // Active should NOT be projected (not in select)
    try std.testing.expect(matches.items[0].get("active") == null);
}

test "stream exec: limit stops early" {
    // Build a large-ish inline dataset
    const src =
        \\{"items":[
        \\  {"id":1,"val":10},{"id":2,"val":20},{"id":3,"val":30},
        \\  {"id":4,"val":40},{"id":5,"val":50},{"id":6,"val":60}
        \\]}
    ;
    var reader2 = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "[items.*] select id limit 3", null);
    defer q.deinit(allocator);

    var sc = try stream.Scanner.init(allocator, &reader2);
    defer sc.deinit();

    const scope = q.scope_pattern.?;
    const star_pos = std.mem.lastIndexOfScalar(u8, scope, '*') orelse scope.len;
    const nav_path = if (star_pos >= 2 and scope[star_pos - 1] == '.')
        scope[0 .. star_pos - 1]
    else
        scope;
    _ = try navigateToArray(&sc, nav_path);

    const lim = q.limit orelse std.math.maxInt(usize);
    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    while (true) {
        const ev = try sc.next();
        if (ev == .array_end or ev == .end_of_input) break;
        if (ev != .object_start) continue;
        var rec = try collectRecord(allocator, &sc);
        defer rec.deinit();
        const out_rec = if (q.fields) |s| try projectRecord(allocator, &rec, s) else try copyRecord(allocator, &rec);
        try matches.append(out_rec);
        if (matches.items.len >= lim) break; // early stop
    }

    try std.testing.expectEqual(@as(usize, 3), matches.items.len);
    try std.testing.expectApproxEqAbs(@as(f64, 1), matches.items[0].get("id").?.number.f, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 3), matches.items[2].get("id").?.number.f, 0.01);
}

test "stream exec: nested field access in where and select" {
    const src =
        \\{"users":[
        \\  {"name":"Alice","address":{"city":"Austin","zip":"78701"},"score":98},
        \\  {"name":"Bob",  "address":{"city":"Boston","zip":"02101"},"score":45},
        \\  {"name":"Eve",  "address":{"city":"Austin","zip":"78702"},"score":91}
        \\]}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "[users.*] select name, address.city where address.city = \"Austin\"", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStream(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);

    const out = writer.writer.buffered();
    // Both Alice and Eve should appear; Bob (Boston) should not
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Eve") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Bob") == null);
    // The city field should be present in output
    try std.testing.expect(std.mem.indexOf(u8, out, "Austin") != null);
}

test "stream exec: NDJSON filter and select" {
    const src =
        \\{"name":"Alice","score":98,"active":true}
        \\{"name":"Bob","score":45,"active":false}
        \\{"name":"Eve","score":91,"active":true}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    // No scope for NDJSON
    const q = try query.parse(allocator, "select name, score where active = true order by score desc", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Eve") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Bob") == null);
}

test "stream exec: NDJSON limit stops early" {
    const src =
        \\{"id":1,"active":true}
        \\{"id":2,"active":true}
        \\{"id":3,"active":true}
        \\{"id":4,"active":true}
        \\{"id":5,"active":true}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select id limit 2", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);
}

test "stream exec: raw mode emits bare values" {
    const src =
        \\{"name":"Alice","score":98,"active":true}
        \\{"name":"Bob","score":45,"active":false}
        \\{"name":"Eve","score":91,"active":true}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select name where active = true", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{ .raw = true });
    try std.testing.expectEqual(@as(usize, 2), n);

    const out = writer.writer.buffered();
    // Raw mode: bare names, no JSON structure
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Eve") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "{") == null); // no JSON objects
}

test "stream exec: group by count NDJSON" {
    const src =
        \\{"level":"error","service":"api"}
        \\{"level":"warn","service":"api"}
        \\{"level":"error","service":"db"}
        \\{"level":"error","service":"api"}
        \\{"level":"info","service":"api"}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select level, count() group by level order by count desc", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 3), n); // error, warn, info

    const out = writer.writer.buffered();
    // "error" should appear first (count=3) — check it's present
    try std.testing.expect(std.mem.indexOf(u8, out, "error") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "warn") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "info") != null);
    // Count field should be present
    try std.testing.expect(std.mem.indexOf(u8, out, "count") != null);
}

test "stream exec: group by count with alias and limit" {
    const src =
        \\{"status":"ok","svc":"a"}
        \\{"status":"ok","svc":"b"}
        \\{"status":"err","svc":"a"}
        \\{"status":"ok","svc":"a"}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select status, count() as total group by status order by total desc limit 1", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n); // only top result

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "ok") != null); // "ok" has count 3
    try std.testing.expect(std.mem.indexOf(u8, out, "total") != null); // alias used
    try std.testing.expect(std.mem.indexOf(u8, out, "err") == null); // not in top 1
}

test "nested: NDJSON select dotted field, output uses leaf key" {
    const src =
        \\{"name":"Alice","address":{"city":"Austin","zip":"78701"},"score":98}
        \\{"name":"Bob","address":{"city":"Boston","zip":"02101"},"score":45}
        \\{"name":"Eve","address":{"city":"Austin","zip":"78702"},"score":91}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select name, address.city where address.city = \"Austin\"", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Eve") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Bob") == null);
    // Output key is leaf "city", not the dotted "address.city"
    try std.testing.expect(std.mem.indexOf(u8, out, "\"city\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "address.city") == null);
}

test "nested: 3-level deep access" {
    const src =
        \\{"name":"Alice","meta":{"region":"us","tier":{"level":"gold","score":99}}}
        \\{"name":"Bob","meta":{"region":"eu","tier":{"level":"silver","score":70}}}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select name, meta.tier.level where meta.region = \"us\"", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n);

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "gold") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Bob") == null);
    // leaf key in output
    try std.testing.expect(std.mem.indexOf(u8, out, "\"level\"") != null);
}

test "nested: alias for dotted field" {
    const src =
        \\{"user":{"name":"Alice","score":99}}
        \\{"user":{"name":"Bob","score":55}}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select user.name as username, user.score as pts", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "username") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "pts") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    // Neither dotted key nor default leaf key should appear
    try std.testing.expect(std.mem.indexOf(u8, out, "\"name\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"score\"") == null);
}

test "nested: select * includes nested objects, excludes dotted keys" {
    const src =
        \\{"name":"Alice","address":{"city":"Austin"},"score":98}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select *", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n);

    const out = writer.writer.buffered();
    // Top-level fields present
    try std.testing.expect(std.mem.indexOf(u8, out, "\"name\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"score\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"address\"") != null);
    // Nested object value emitted as raw JSON
    try std.testing.expect(std.mem.indexOf(u8, out, "Austin") != null);
    // Intermediate dotted key must NOT appear in output
    try std.testing.expect(std.mem.indexOf(u8, out, "address.city") == null);
}

test "nested: select whole nested object" {
    const src =
        \\{"name":"Alice","address":{"city":"Austin","zip":"78701"},"score":98}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select address", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n);

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"address\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Austin") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "78701") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") == null); // not selected
}

test "nested: order by dotted field" {
    const src =
        \\{"name":"Charlie","address":{"city":"Chicago"}}
        \\{"name":"Alice","address":{"city":"Abilene"}}
        \\{"name":"Bob","address":{"city":"Baltimore"}}
        \\
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;

    const q = try query.parse(allocator, "select name, address.city order by address.city asc", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 3), n);

    const out = writer.writer.buffered();
    // Abilene < Baltimore < Chicago alphabetically
    const pos_alice = std.mem.indexOf(u8, out, "Alice").?;
    const pos_bob = std.mem.indexOf(u8, out, "Bob").?;
    const pos_charlie = std.mem.indexOf(u8, out, "Charlie").?;
    try std.testing.expect(pos_alice < pos_bob);
    try std.testing.expect(pos_bob < pos_charlie);
}

test "string ops: contains and not_contains" {
    const src =
        \\{"id":1,"level":"error","msg":"disk full"}
        \\{"id":2,"level":"warn","msg":"low memory"}
        \\{"id":3,"level":"info","msg":"started"}
        \\{"id":4,"level":"error","msg":"network error"}
        \\
    ;
    const allocator = std.testing.allocator;

    // contains
    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select id where level contains \"error\"", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 2), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 1") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 4") != null);
    }

    // not contains
    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select id where level not contains \"error\"", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 2), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 2") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 3") != null);
    }
}

test "string ops: starts_with and ends_with" {
    const src =
        \\{"name":"Alice"}
        \\{"name":"Bob"}
        \\{"name":"Alicia"}
        \\{"name":"Bobby"}
        \\{"name":"Charlie"}
        \\
    ;
    const allocator = std.testing.allocator;

    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select name where name starts_with \"Al\"", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 2), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "Alicia") != null);
    }

    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select name where name ends_with \"y\"", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 1), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "Bobby") != null);
    }
}

test "string ops: in and not in list" {
    const src =
        \\{"id":1,"status":"active"}
        \\{"id":2,"status":"pending"}
        \\{"id":3,"status":"inactive"}
        \\{"id":4,"status":"active"}
        \\
    ;
    const allocator = std.testing.allocator;

    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select id where status in (\"active\", \"pending\")", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 3), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 1") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 2") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 4") != null);
    }

    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select id where status not in (\"active\", \"pending\")", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 1), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 3") != null);
    }
}

test "null ops: is null and is not null" {
    const src =
        \\{"id":1,"region":"us-east"}
        \\{"id":2,"region":null}
        \\{"id":3}
        \\{"id":4,"region":"eu-west"}
        \\
    ;
    const allocator = std.testing.allocator;

    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select id where region is null", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 2), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 2") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 3") != null);
    }

    {
        var r = std.Io.Reader.fixed(src);
        const q = try query.parse(allocator, "select id where region is not null", null);
        defer q.deinit(allocator);
        var writer = std.Io.Writer.Allocating.init(allocator);
        defer allocator.free(writer.writer.buffer);
        const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
        try std.testing.expectEqual(@as(usize, 2), n);
        const out = writer.writer.buffered();
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 1") != null);
        try std.testing.expect(std.mem.indexOf(u8, out, "\"id\": 4") != null);
    }
}

test "isnull: default value for missing or null fields" {
    const src =
        \\{"id":1,"region":"us-east"}
        \\{"id":2,"region":null}
        \\{"id":3}
        \\
    ;
    const allocator = std.testing.allocator;

    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select id, isnull(region, \"unknown\") as region", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 3), n);
    const out = writer.writer.buffered();

    // id=1 keeps original region
    try std.testing.expect(std.mem.indexOf(u8, out, "\"us-east\"") != null);
    // id=2 and id=3: "unknown" appears twice
    var count: usize = 0;
    var search = out;
    while (std.mem.indexOf(u8, search, "\"unknown\"")) |pos| {
        count += 1;
        search = search[pos + 1 ..];
    }
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "isnull: with group by on coalesced field" {
    const src =
        \\{"user":"alice","region":"us"}
        \\{"user":"bob","region":null}
        \\{"user":"carol"}
        \\{"user":"dave","region":"eu"}
        \\{"user":"eve","region":"us"}
        \\
    ;
    const allocator = std.testing.allocator;

    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select isnull(region, \"unknown\") as region, count() group by region", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 3), n); // us, eu, unknown
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"unknown\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"us\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"eu\"") != null);
}

// ─── Expression / scalar function tests ──────────────────────────────────────

test "expr: arithmetic select — price * qty as total" {
    const src =
        \\{"name":"a","price":10.5,"qty":3}
        \\{"name":"b","price":20,"qty":2}
        \\{"name":"c","price":5,"qty":10}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select price * qty as total", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "31.5") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "40") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "50") != null);
}

test "expr: upper() and lower() in select" {
    const src =
        \\{"name":"alice","city":"NEW YORK"}
        \\{"name":"BOB","city":"los angeles"}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select upper(name) as name, lower(city) as city", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"ALICE\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"BOB\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"new york\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"los angeles\"") != null);
}

test "expr: round(), floor(), ceil(), abs() in select" {
    const src =
        \\{"score":7.856,"neg":-3.7}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select round(score, 1) as r, floor(score) as fl, ceil(score) as ce, abs(neg) as ab", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"r\": 7.9") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"fl\": 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"ce\": 8") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"ab\": 3.7") != null);
}

test "expr: concat() in select" {
    const src =
        \\{"first":"John","last":"Doe"}
        \\{"first":"Jane","last":"Smith"}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select concat(first, last) as fullname", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"JohnDoe\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"JaneSmith\"") != null);
}

test "expr: substr() in select" {
    const src =
        \\{"code":"ABCDEF"}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select substr(code, 1, 3) as part", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"BCD\"") != null);
}

test "expr: trim() in select" {
    const src =
        \\{"msg":"  hello  "}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select trim(msg) as msg", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"hello\"") != null);
}

test "expr: to_str() in select" {
    const src =
        \\{"score":88.5,"active":true}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select to_str(score) as score_str, to_str(active) as active_str", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"88.5\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"true\"") != null);
}

test "expr: len() in WHERE" {
    const src =
        \\{"name":"Al","score":90}
        \\{"name":"Alice","score":80}
        \\{"name":"Bob","score":70}
        \\{"name":"Charlie","score":60}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select name where len(name) > 4", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Alice\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Charlie\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Al\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Bob\"") == null);
}

test "expr: upper() in WHERE" {
    const src =
        \\{"status":"active","val":1}
        \\{"status":"INACTIVE","val":2}
        \\{"status":"Active","val":3}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select val where upper(status) = \"ACTIVE\"", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"val\": 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"val\": 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"val\": 2") == null);
}

// ── Aggregate function tests ──────────────────────────────────────────────────

test "agg: sum group by" {
    const src =
        \\{"dept":"eng","salary":120000}
        \\{"dept":"sales","salary":85000}
        \\{"dept":"eng","salary":135000}
        \\{"dept":"sales","salary":78000}
        \\{"dept":"eng","salary":128000}
        \\{"dept":"hr","salary":65000}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select dept, sum(salary) as total group by dept", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "383000") != null); // eng: 120000+135000+128000
    try std.testing.expect(std.mem.indexOf(u8, out, "163000") != null); // sales: 85000+78000
    try std.testing.expect(std.mem.indexOf(u8, out, "65000") != null); // hr
}

test "agg: avg group by" {
    const src =
        \\{"dept":"eng","salary":100}
        \\{"dept":"eng","salary":200}
        \\{"dept":"eng","salary":300}
        \\{"dept":"hr","salary":50}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select dept, avg(salary) as avg_sal group by dept", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "200") != null); // eng avg
    try std.testing.expect(std.mem.indexOf(u8, out, "50") != null); // hr avg
}

test "agg: min and max group by" {
    const src =
        \\{"dept":"eng","score":88}
        \\{"dept":"eng","score":95}
        \\{"dept":"eng","score":72}
        \\{"dept":"hr","score":60}
        \\{"dept":"hr","score":77}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select dept, min(score) as lo, max(score) as hi group by dept", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    // eng: min=72, max=95  |  hr: min=60, max=77
    try std.testing.expect(std.mem.indexOf(u8, out, "72") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "95") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "60") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "77") != null);
}

test "agg: sum avg min max count combined" {
    const src =
        \\{"dept":"eng","salary":120000}
        \\{"dept":"sales","salary":85000}
        \\{"dept":"eng","salary":135000}
        \\{"dept":"sales","salary":78000}
        \\{"dept":"eng","salary":128000}
        \\{"dept":"hr","salary":65000}
    ;
    var r = std.Io.Reader.fixed(src);
    const allocator = std.testing.allocator;
    const q = try query.parse(allocator, "select dept, sum(salary) as total, avg(salary) as avg_sal, " ++
        "min(salary) as lo, max(salary) as hi, count() group by dept", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execGroupByNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    // eng: total=383000, lo=120000, hi=135000
    try std.testing.expect(std.mem.indexOf(u8, out, "383000") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "120000") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "135000") != null);
    // sales: total=163000
    try std.testing.expect(std.mem.indexOf(u8, out, "163000") != null);
}

// ─── Large-line slow-path tests ──────────────────────────────────────────────

test "large lines: records after a >64KB line are not dropped" {
    // A line longer than the 64KB read buffer used to kill the entire loop —
    // the slow-path re-called takeDelimiter which infinitely returned StreamTooLong
    // without advancing the reader.  The fix uses peek+toss to drain the line.
    const allocator = std.testing.allocator;

    // Build test input: small record, 70KB record, small record, 10KB record
    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();

    const blob70k: [70_000]u8 = @splat('x');
    const blob10k: [10_000]u8 = @splat('x');
    try buf.appendSlice("{\"id\":\"before\",\"score\":1}\n");
    try buf.appendSlice("{\"id\":\"big70k\",\"score\":2,\"blob\":\"");
    try buf.appendSlice(&blob70k);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"middle\",\"score\":3}\n");
    try buf.appendSlice("{\"id\":\"big10k\",\"score\":4,\"blob\":\"");
    try buf.appendSlice(&blob10k);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"after\",\"score\":5}\n");

    var r = std.Io.Reader.fixed(buf.items);
    const q = try query.parse(allocator, "select id, score", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});

    // All 5 records must survive (large objects are projected down to just id+score)
    try std.testing.expectEqual(@as(usize, 5), n);
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"before\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"big70k\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"middle\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"big10k\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"after\"") != null);
}

test "large lines: WHERE filter works across large records" {
    const allocator = std.testing.allocator;

    var buf = std.array_list.Managed(u8).init(allocator);
    defer buf.deinit();

    const blob: [70_000]u8 = @splat('y');
    try buf.appendSlice("{\"id\":\"a\",\"dept\":\"eng\",\"blob\":\"");
    try buf.appendSlice(&blob);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"b\",\"dept\":\"hr\",\"blob\":\"");
    try buf.appendSlice(&blob);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"c\",\"dept\":\"eng\",\"blob\":\"");
    try buf.appendSlice(&blob);
    try buf.appendSlice("\"}\n");

    var r = std.Io.Reader.fixed(buf.items);
    const q = try query.parse(allocator, "select id where dept = \"eng\"", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});

    try std.testing.expectEqual(@as(usize, 2), n);
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"c\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"b\"") == null);
}

// ─── Split-pipe tests (--tee / --reject) ─────────────────────────────────────

test "split-pipe: --reject collects non-matching records" {
    const src =
        \\{"id":1,"status":"active","name":"Alice"}
        \\{"id":2,"status":"inactive","name":"Bob"}
        \\{"id":3,"status":"active","name":"Carol"}
        \\{"id":4,"status":"inactive","name":"Dave"}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    var reject_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(reject_buf.writer.buffer);

    var stdout_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(stdout_buf.writer.buffer);

    const q = try query.parse(allocator, "where status = \"active\"", null);
    defer q.deinit(allocator);

    const opts = ExecOptions{ .reject_writer = &reject_buf.writer };
    const n = try execStreamNDJSON(allocator, &r, q, &stdout_buf.writer, opts);
    try std.testing.expectEqual(@as(usize, 2), n);

    // stdout should have Alice and Carol
    const out = stdout_buf.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Carol") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Bob") == null);

    // reject file should have Bob and Dave as raw NDJSON lines
    const rejected = reject_buf.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rejected, "Bob") != null);
    try std.testing.expect(std.mem.indexOf(u8, rejected, "Dave") != null);
    try std.testing.expect(std.mem.indexOf(u8, rejected, "Alice") == null);
    // Verify raw NDJSON format (original JSON, not re-serialised)
    try std.testing.expect(std.mem.indexOf(u8, rejected, "\"id\":2") != null);
}

test "split-pipe: --tee copies all records regardless of filter" {
    const src =
        \\{"id":1,"ok":true}
        \\{"id":2,"ok":false}
        \\{"id":3,"ok":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    var tee_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(tee_buf.writer.buffer);

    var stdout_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(stdout_buf.writer.buffer);

    const q = try query.parse(allocator, "where ok = true", null);
    defer q.deinit(allocator);

    const opts = ExecOptions{ .tee_writer = &tee_buf.writer };
    const n = try execStreamNDJSON(allocator, &r, q, &stdout_buf.writer, opts);
    try std.testing.expectEqual(@as(usize, 2), n);

    // tee should have all 3 records
    const tee_out = tee_buf.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, tee_out, "\"id\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, tee_out, "\"id\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, tee_out, "\"id\":3") != null);
}

test "split-pipe: --tee and --reject together, 3-way split" {
    const src =
        \\{"id":1,"v":true}
        \\{"id":2,"v":false}
        \\{"id":3,"v":true}
        \\{"id":4,"v":false}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    var tee_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(tee_buf.writer.buffer);

    var reject_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(reject_buf.writer.buffer);

    var stdout_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(stdout_buf.writer.buffer);

    const q = try query.parse(allocator, "where v = true", null);
    defer q.deinit(allocator);

    const opts = ExecOptions{
        .tee_writer = &tee_buf.writer,
        .reject_writer = &reject_buf.writer,
    };
    const n = try execStreamNDJSON(allocator, &r, q, &stdout_buf.writer, opts);
    try std.testing.expectEqual(@as(usize, 2), n);

    // tee = all 4
    const tee = tee_buf.writer.buffered();
    var tee_lines: usize = 0;
    var it = std.mem.splitScalar(u8, std.mem.trimEnd(u8, tee, "\n"), '\n');
    while (it.next()) |_| tee_lines += 1;
    try std.testing.expectEqual(@as(usize, 4), tee_lines);

    // reject = 2 (id:2, id:4)
    const rej = reject_buf.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, rej, "\"id\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, rej, "\"id\":4") != null);
    try std.testing.expect(std.mem.indexOf(u8, rej, "\"id\":1") == null);
}

test "split-pipe: no WHERE clause means zero rejects" {
    const src =
        \\{"id":1}
        \\{"id":2}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    var reject_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(reject_buf.writer.buffer);

    var stdout_buf = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(stdout_buf.writer.buffer);

    const q = try query.parse(allocator, "select id", null);
    defer q.deinit(allocator);

    const opts = ExecOptions{ .reject_writer = &reject_buf.writer };
    const n = try execStreamNDJSON(allocator, &r, q, &stdout_buf.writer, opts);
    try std.testing.expectEqual(@as(usize, 2), n);

    // With no WHERE, nothing is rejected
    const rej = reject_buf.writer.buffered();
    try std.testing.expectEqual(@as(usize, 0), rej.len);
}

// ─── CSV output tests ────────────────────────────────────────────────────────

test "output: CSV format" {
    const src =
        \\{"name":"Alice","score":98}
        \\{"name":"Bob","score":45}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select name, score", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{ .format = .csv });
    const out = writer.writer.buffered();
    // Should contain CSV header and rows
    try std.testing.expect(std.mem.indexOf(u8, out, "name,score") != null or
        std.mem.indexOf(u8, out, "score,name") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Bob") != null);
}

test "output: TSV format" {
    const src =
        \\{"name":"Alice","score":98}
        \\{"name":"Bob","score":45}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select name, score", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{ .format = .tsv });
    const out = writer.writer.buffered();
    // TSV uses tabs
    try std.testing.expect(std.mem.indexOf(u8, out, "\t") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "Alice") != null);
}

// ─── Global aggregation tests ────────────────────────────────────────────────

test "global agg: avg, sum, count over all records" {
    const src =
        \\{"score":10}
        \\{"score":20}
        \\{"score":30}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select avg(score), sum(score), count()", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    _ = try execGlobalAggNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "20") != null); // avg
    try std.testing.expect(std.mem.indexOf(u8, out, "60") != null); // sum
    try std.testing.expect(std.mem.indexOf(u8, out, "3") != null); // count
}

// ─── DISTINCT tests ──────────────────────────────────────────────────────────

test "distinct: removes duplicate rows" {
    const src =
        \\{"city":"Austin"}
        \\{"city":"Boston"}
        \\{"city":"Austin"}
        \\{"city":"Chicago"}
        \\{"city":"Boston"}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select distinct city", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 3), n); // Austin, Boston, Chicago
}

// ─── has() / not has() tests ─────────────────────────────────────────────────

test "has: filter records with existing key" {
    const src =
        \\{"id":1,"email":"a@b.com"}
        \\{"id":2}
        \\{"id":3,"email":"c@d.com"}
        \\{"id":4,"email":null}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select id where has(email)", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    // has() checks key existence — id:1, id:3, and id:4 (even null) have the key
    try std.testing.expect(n >= 2);
}

// ─── LIKE (glob) tests ──────────────────────────────────────────────────────

test "like: glob pattern matching" {
    const src =
        \\{"city":"New York"}
        \\{"city":"New Orleans"}
        \\{"city":"Boston"}
        \\{"city":"Newark"}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select city where city like \"New%\"", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 3), n); // New York, New Orleans, Newark

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "Boston") == null);
}

// ─── AND/OR multi-condition tests ────────────────────────────────────────────

test "multi-condition: AND + OR combined" {
    const src =
        \\{"name":"A","score":90,"active":true}
        \\{"name":"B","score":50,"active":true}
        \\{"name":"C","score":95,"active":false}
        \\{"name":"D","score":10,"active":false}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select name where active = true and score > 60", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n); // only A

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"A\"") != null);
}

// ─── Edge case tests ─────────────────────────────────────────────────────────

test "edge: empty input produces empty output" {
    const src = "";
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select id", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "edge: blank lines between records are skipped" {
    const src =
        \\{"id":1}
        \\
        \\
        \\{"id":2}
        \\
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select id", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);
}

test "edge: select * passes all fields through" {
    const src =
        \\{"a":1,"b":"hello","c":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select *", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n);

    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"b\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"c\"") != null);
}

test "edge: numeric comparison with negative numbers" {
    const src =
        \\{"temp":-5}
        \\{"temp":0}
        \\{"temp":10}
        \\{"temp":-20}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select temp where temp < 0", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);
}

test "edge: order by desc + limit combined" {
    const src =
        \\{"name":"A","score":10}
        \\{"name":"B","score":50}
        \\{"name":"C","score":30}
        \\{"name":"D","score":90}
        \\{"name":"E","score":70}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);

    const q = try query.parse(allocator, "select name order by score desc limit 2", null);
    defer q.deinit(allocator);

    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);

    const out = writer.writer.buffered();
    // D (90) should come first, then E (70)
    const pos_d = std.mem.indexOf(u8, out, "\"D\"").?;
    const pos_e = std.mem.indexOf(u8, out, "\"E\"").?;
    try std.testing.expect(pos_d < pos_e);
}

// ─── replace() function test ────────────────────────────────────────────────

test "expr: replace() in select" {
    const src =
        \\{"msg":"hello world"}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select replace(msg, \"world\", \"zig\") as msg", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "hello zig") != null);
}

// ─── coalesce() test ────────────────────────────────────────────────────────

test "expr: coalesce() picks first non-null" {
    const src =
        \\{"a":null,"b":"yes","c":"no"}
        \\{"a":"first","b":"second","c":"third"}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select coalesce(a, b, c) as result", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execStreamNDJSON(allocator, &r, q, &writer.writer, .{});
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"yes\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"first\"") != null);
}

// ═══════════════════════════════════════════════════════════════════════════
// ─── LLM token-accumulation mode tests ─────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════

test "extractNextObject: single complete object" {
    const buf = "{\"name\":\"Alice\",\"age\":30}";
    const span = extractNextObject(buf).?;
    try std.testing.expectEqual(@as(usize, 0), span.start);
    try std.testing.expectEqual(buf.len, span.end);
}

test "extractNextObject: leading garbage before object" {
    const buf = "   some garbage  {\"x\":1}  trailing";
    const span = extractNextObject(buf).?;
    try std.testing.expectEqualStrings("{\"x\":1}", buf[span.start..span.end]);
}

test "extractNextObject: nested braces in object" {
    const buf = "{\"a\":{\"b\":{\"c\":1}}}rest";
    const span = extractNextObject(buf).?;
    try std.testing.expectEqualStrings("{\"a\":{\"b\":{\"c\":1}}}", buf[span.start..span.end]);
}

test "extractNextObject: braces inside quoted strings" {
    const buf = "{\"msg\":\"hello {world}\",\"x\":1}";
    const span = extractNextObject(buf).?;
    try std.testing.expectEqualStrings(buf, buf[span.start..span.end]);
}

test "extractNextObject: escaped quotes in strings" {
    const buf = "{\"s\":\"a\\\"b}\",\"x\":1}";
    const span = extractNextObject(buf).?;
    try std.testing.expectEqualStrings(buf, buf[span.start..span.end]);
}

test "extractNextObject: incomplete object returns null" {
    const buf = "{\"name\":\"Alice";
    try std.testing.expect(extractNextObject(buf) == null);
}

test "extractNextObject: two objects back-to-back" {
    const buf = "{\"a\":1}{\"b\":2}";
    const span1 = extractNextObject(buf).?;
    try std.testing.expectEqualStrings("{\"a\":1}", buf[span1.start..span1.end]);
    // After draining first object, second should be found
    const rest = buf[span1.end..];
    const span2 = extractNextObject(rest).?;
    try std.testing.expectEqualStrings("{\"b\":2}", rest[span2.start..span2.end]);
}

test "drainAccum: removes consumed bytes" {
    var accum = std.array_list.Managed(u8).init(std.testing.allocator);
    defer accum.deinit();
    try accum.appendSlice("{\"a\":1}{\"b\":2}");
    drainAccum(&accum, 7); // remove first object
    try std.testing.expectEqualStrings("{\"b\":2}", accum.items);
}

test "llm stream: basic token accumulation select" {
    // Simulate Ollama-style NDJSON with fragmented response field
    const src =
        \\{"response":"{\"na","done":false}
        \\{"response":"me\":\"Al","done":false}
        \\{"response":"ice\",\"age\":30}","done":false}
        \\{"response":"","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select name, age", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmStream(allocator, &r, q, &writer.writer, .{ .llm_field = "response" });
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Alice\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "30") != null);
}

test "llm stream: multiple objects from fragments" {
    const src =
        \\{"response":"{\"x\":1}{\"x\":2}","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select x", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmStream(allocator, &r, q, &writer.writer, .{ .llm_field = "response" });
    const out = writer.writer.buffered();
    // Should find both objects
    var count: usize = 0;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, out, pos, "\"x\"")) |p| {
        count += 1;
        pos = p + 1;
    }
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "llm stream: WHERE filter" {
    const src =
        \\{"response":"{\"name\":\"Alice\",\"age\":30}{\"name\":\"Bob\",\"age\":20}","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select name where age > 25", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmStream(allocator, &r, q, &writer.writer, .{ .llm_field = "response" });
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Alice\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"Bob\"") == null);
}

test "llm global agg: count and avg" {
    const src =
        \\{"response":"{\"v\":10}{\"v\":20}{\"v\":30}","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select count, avg(v) as avg_v", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmGlobalAgg(allocator, &r, q, &writer.writer, .{ .llm_field = "response" });
    const out = writer.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "3") != null); // count=3
    try std.testing.expect(std.mem.indexOf(u8, out, "20") != null); // avg=20
}

test "llm group by: aggregate by category" {
    const src =
        \\{"response":"{\"cat\":\"a\",\"v\":10}{\"cat\":\"b\",\"v\":20}{\"cat\":\"a\",\"v\":30}","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select cat, count, sum(v) as total group by cat", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmGroupBy(allocator, &r, q, &writer.writer, .{ .llm_field = "response" });
    const out = writer.writer.buffered();
    // Group "a": count=2, sum=40; Group "b": count=1, sum=20
    try std.testing.expect(std.mem.indexOf(u8, out, "40") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"b\"") != null);
}

test "llm rolling global agg: emits snapshot per object" {
    // 3 objects delivered in one burst → 3 rolling NDJSON lines
    const src =
        \\{"response":"{\"v\":10}{\"v\":20}{\"v\":30}","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select count, avg(v) as avg_v", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmGlobalAgg(allocator, &r, q, &writer.writer, .{ .llm_field = "response", .rolling = true });
    const out = writer.writer.buffered();
    // Should be 3 lines of NDJSON (one per object), no array wrapper
    var line_count: usize = 0;
    var it = std.mem.splitScalar(u8, std.mem.trimEnd(u8, out, "\n"), '\n');
    while (it.next()) |line| {
        if (line.len == 0) continue;
        line_count += 1;
        // Each line must start with '{' (a JSON object)
        try std.testing.expect(line[0] == '{');
    }
    try std.testing.expectEqual(@as(usize, 3), line_count);
    // First snapshot should have count=1, last should have count=3
    try std.testing.expect(std.mem.indexOf(u8, out, "\"count\": 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"count\": 3") != null);
}

test "llm rolling group by: emits snapshot per object" {
    const src =
        \\{"response":"{\"cat\":\"a\",\"v\":10}{\"cat\":\"b\",\"v\":20}{\"cat\":\"a\",\"v\":30}","done":true}
    ;
    const allocator = std.testing.allocator;
    var r = std.Io.Reader.fixed(src);
    const q = try query.parse(allocator, "select cat, count, sum(v) as total group by cat", null);
    defer q.deinit(allocator);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);
    _ = try execLlmGroupBy(allocator, &r, q, &writer.writer, .{ .llm_field = "response", .rolling = true });
    const out = writer.writer.buffered();
    // Should be 3 snapshot lines (one per object ingested)
    var line_count: usize = 0;
    var it = std.mem.splitScalar(u8, std.mem.trimEnd(u8, out, "\n"), '\n');
    while (it.next()) |line| {
        if (line.len == 0) continue;
        line_count += 1;
        // Each line must start with '[' (a JSON array of groups)
        try std.testing.expect(line[0] == '[');
    }
    try std.testing.expectEqual(@as(usize, 3), line_count);
    // First snapshot: just group "a", last snapshot: groups "a" and "b"
    // The final line should contain total=40 for group "a"
    try std.testing.expect(std.mem.indexOf(u8, out, "40") != null);
}

// ─── WHERE matches operator tests ────────────────────────────────────────────

test "ndjson: WHERE matches regex filter" {
    const allocator = std.testing.allocator;
    const input =
        \\{"name":"alice","code":"ERR-123"}
        \\{"name":"bob","code":"OK-456"}
        \\{"name":"carol","code":"ERR-789"}
        \\
    ;
    const q = try query.parse(allocator, "select name where code matches 'ERR-\\d+'", null);
    defer q.deinit(allocator);

    var in_reader = std.Io.Reader.fixed(input);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &in_reader, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 2), n);
}

test "ndjson: WHERE not matches regex filter" {
    const allocator = std.testing.allocator;
    const input =
        \\{"name":"alice","code":"ERR-123"}
        \\{"name":"bob","code":"OK-456"}
        \\{"name":"carol","code":"ERR-789"}
        \\
    ;
    const q = try query.parse(allocator, "select name where code not matches 'ERR-\\d+'", null);
    defer q.deinit(allocator);

    var in_reader = std.Io.Reader.fixed(input);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execStreamNDJSON(allocator, &in_reader, q, &writer.writer, .{});
    try std.testing.expectEqual(@as(usize, 1), n);
}

// ─── LLM with nested path extraction test ───────────────────────────────────

test "llm stream: nested path extraction (openai-style)" {
    const allocator = std.testing.allocator;
    // Simulate OpenAI SSE-style input (without data: prefix — just NDJSON with nested structure)
    const input =
        \\{"choices":[{"delta":{"content":"{\"name\":\"alice\","}}]}
        \\{"choices":[{"delta":{"content":"\"age\":30}"}}]}
        \\
    ;
    const q = try query.parse(allocator, "select name", null);
    defer q.deinit(allocator);

    var in_reader = std.Io.Reader.fixed(input);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execLlmStream(allocator, &in_reader, q, &writer.writer, .{
        .llm_path = "choices.0.delta.content",
    });
    try std.testing.expectEqual(@as(usize, 1), n);
}

// ─── LLM with SSE framing test ──────────────────────────────────────────────

test "llm stream: SSE framing with data: prefix" {
    const allocator = std.testing.allocator;
    // Simulate SSE stream
    const input =
        \\: keep-alive
        \\data: {"choices":[{"delta":{"content":"{\"x\":1}"}}]}
        \\data: [DONE]
        \\
    ;
    const q = try query.parse(allocator, "select x", null);
    defer q.deinit(allocator);

    var in_reader = std.Io.Reader.fixed(input);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execLlmStream(allocator, &in_reader, q, &writer.writer, .{
        .llm_path = "choices.0.delta.content",
        .api_mode = .openai,
    });
    try std.testing.expectEqual(@as(usize, 1), n);
}

// ─── LLM with schema validation test ────────────────────────────────────────

test "llm stream: schema validation rejects invalid objects" {
    const allocator = std.testing.allocator;
    const input =
        \\{"response":"{\"name\":\"alice\",\"price\":42}"}
        \\{"response":"{\"name\":123,\"price\":\"bad\"}"}
        \\{"response":"{\"name\":\"bob\",\"price\":99}"}
        \\
    ;
    const q = try query.parse(allocator, "select name, price", null);
    defer q.deinit(allocator);

    const schema = try parseExpectSchema(allocator, "name:string,price:number");
    defer allocator.free(schema);

    var in_reader = std.Io.Reader.fixed(input);
    var writer = std.Io.Writer.Allocating.init(allocator);
    defer allocator.free(writer.writer.buffer);

    const n = try execLlmStream(allocator, &in_reader, q, &writer.writer, .{
        .llm_field = "response",
        .expect_schema = schema,
    });
    // Object 2 has name:number and price:string — should be rejected
    try std.testing.expectEqual(@as(usize, 2), n);
}
