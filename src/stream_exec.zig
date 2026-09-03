//! stream_exec.zig — streaming query executor
//!
//! Executes a kq Query against a JSON stream without buffering the whole document.
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
const query = @import("kq_query");
const stream = @import("kq_stream");
const regex = @import("kq_regex");
const record_mod = @import("kq_record");
const expr_mod = @import("kq_expr");
const where_mod = @import("kq_where");
const output_mod = @import("kq_output");
const llm_mod = @import("kq_llm");
const raw_where_mod = @import("kq_raw_where");

const simpleRegexMatch = regex.simpleRegexMatch;
const globLike = regex.globLike;

// Re-export record types for backward compatibility (record_source.zig, main.zig, etc.)
pub const OwnedValue = record_mod.OwnedValue;
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
const emitAggSnapshot = llm_mod.emitAggSnapshot;
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
            // evalExpr already allocates strings into `allocator` — take ownership directly
            const val = evalExpr(allocator, src, e) orelse {
                allocator.free(out_key);
                continue;
            };
            const out_val: OwnedValue = switch (val) {
                // strings/raws already owned by `allocator` — no second dupe needed
                .string => |s| .{ .string = s },
                .number => |n| .{ .number = n },
                .boolean => |b| .{ .boolean = b },
                .null_val => .null_val,
                .raw => |r| .{ .raw = r },
            };
            try out.fields.put(allocator, out_key, out_val);
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
fn collectObjectInto(
    allocator: std.mem.Allocator,
    sc: anytype,
    rec: *Record,
    prefix: []const u8,
) !void {
    field_loop: while (true) {
        const ev = try sc.next();
        switch (ev) {
            .object_end => break :field_loop,
            .key => |k| {
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
                        // We need to iterate child events both for raw and for recursion.
                        // Strategy: collect child field events, build raw, and also insert
                        // dot-path keys for scalar leaves.
                        try collectNestedObject(allocator, sc, rec, full_key, &raw_buf);
                        const raw_bytes = try raw_buf.toOwnedSlice();
                        try rec.fields.put(allocator, full_key, .{ .raw = raw_bytes });
                    },
                    .array_start => {
                        // Arrays: store as raw only (no nested array element querying yet)
                        var raw_buf = std.array_list.Managed(u8).init(allocator);
                        errdefer raw_buf.deinit();
                        try raw_buf.append('[');
                        try collectRawArray(sc, &raw_buf);
                        const raw_bytes = try raw_buf.toOwnedSlice();
                        // Also store indexed entries key\x1f0, key\x1f1, ... for direct access
                        if (std.json.parseFromSlice(std.json.Value, allocator, raw_bytes, .{})) |parsed| {
                            defer parsed.deinit();
                            if (parsed.value == .array) {
                                for (parsed.value.array.items, 0..) |elem, idx| {
                                    const idx_key = try std.fmt.allocPrint(allocator, "{s}{c}{d}", .{ full_key, PATH_SEP, idx });
                                    errdefer allocator.free(idx_key);
                                    const idx_val: OwnedValue = switch (elem) {
                                        .string => |s| .{ .string = try allocator.dupe(u8, s) },
                                        .integer => |n| .{ .number = @floatFromInt(n) },
                                        .float => |f2| .{ .number = f2 },
                                        .bool => |b| .{ .boolean = b },
                                        .null => .null_val,
                                        else => v: {
                                            var aw = std.Io.Writer.Allocating.init(allocator);
                                            errdefer aw.deinit();
                                            try std.json.Stringify.value(elem, .{}, &aw.writer);
                                            break :v OwnedValue{ .raw = try aw.toOwnedSlice() };
                                        },
                                    };
                                    try rec.fields.put(allocator, idx_key, idx_val);
                                }
                            }
                        } else |_| {} // ignore parse errors — still store raw
                        try rec.fields.put(allocator, full_key, .{ .raw = raw_bytes });
                    },
                    .string => |s| try rec.fields.put(allocator, full_key, .{ .string = try allocator.dupe(u8, s) }),
                    .number => |n| try rec.fields.put(allocator, full_key, .{ .number = n }),
                    .boolean => |b| try rec.fields.put(allocator, full_key, .{ .boolean = b }),
                    .null_val => try rec.fields.put(allocator, full_key, .null_val),
                    else => allocator.free(full_key), // unexpected — release key
                }
            },
            else => {}, // unexpected — skip
        }
    }
}

/// Collect a nested object that was already entered (opening `{` consumed).
/// Simultaneously:
///   - appends raw JSON bytes to `raw_buf` (for output)
///   - inserts flattened dot-path entries into `rec` for scalar leaves
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
        switch (ev) {
            .object_end => {
                try raw_buf.append('}');
                return;
            },
            .key => |k| {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                try raw_buf.append('"');
                try raw_buf.appendSlice(k);
                try raw_buf.appendSlice("\": ");

                const child_key = try std.fmt.allocPrint(allocator, "{s}{c}{s}", .{ prefix, PATH_SEP, k });
                errdefer allocator.free(child_key);

                const vev = try sc.next();
                switch (vev) {
                    .object_start => {
                        // Write '{' into both the parent raw_buf and a separate snapshot
                        // so we can store the nested object as its own .raw value.
                        var snap = std.array_list.Managed(u8).init(allocator);
                        defer snap.deinit();
                        try snap.append('{');
                        try raw_buf.append('{');
                        // Recurse writing into *both* raw_buf and snap in sync.
                        // We use a two-writer helper to keep them in lockstep.
                        try collectNestedObjectDual(allocator, sc, rec, child_key, raw_buf, &snap);
                        try rec.fields.put(allocator, child_key, .{ .raw = try snap.toOwnedSlice() });
                    },
                    .array_start => {
                        // Capture the array raw for both output and storage.
                        var snap = std.array_list.Managed(u8).init(allocator);
                        errdefer snap.deinit();
                        try snap.append('[');
                        try raw_buf.append('[');
                        // Collect into snap; mirror into raw_buf by re-appending snap's new bytes.
                        const before = raw_buf.items.len;
                        try collectRawArray(sc, &snap);
                        // snap now has the full "[...]", append the part after '[' to raw_buf
                        try raw_buf.appendSlice(snap.items[1..]);
                        _ = before;
                        try rec.fields.put(allocator, child_key, .{ .raw = try snap.toOwnedSlice() });
                    },
                    .string => |s| {
                        try raw_buf.append('"');
                        try raw_buf.appendSlice(s);
                        try raw_buf.append('"');
                        try rec.fields.put(allocator, child_key, .{ .string = try allocator.dupe(u8, s) });
                    },
                    .number => |n| {
                        var tmp: [32]u8 = undefined;
                        const ns = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch "0";
                        try raw_buf.appendSlice(ns);
                        try rec.fields.put(allocator, child_key, .{ .number = n });
                    },
                    .boolean => |b| {
                        try raw_buf.appendSlice(if (b) "true" else "false");
                        try rec.fields.put(allocator, child_key, .{ .boolean = b });
                    },
                    .null_val => {
                        try raw_buf.appendSlice("null");
                        try rec.fields.put(allocator, child_key, .null_val);
                    },
                    else => allocator.free(child_key),
                }
            },
            else => {},
        }
    }
}

/// Like collectNestedObject but writes simultaneously into two buffers:
/// `out_buf` (the parent's raw output) and `snap` (a snapshot for storing as .raw).
/// Both buffers must already have '{' appended before calling.
fn collectNestedObjectDual(
    allocator: std.mem.Allocator,
    sc: anytype,
    rec: *Record,
    prefix: []const u8,
    out_buf: *std.array_list.Managed(u8),
    snap: *std.array_list.Managed(u8),
) !void {
    var first = true;
    while (true) {
        const ev = try sc.next();
        switch (ev) {
            .object_end => {
                try out_buf.append('}');
                try snap.append('}');
                return;
            },
            .key => |k| {
                if (!first) {
                    try out_buf.appendSlice(", ");
                    try snap.appendSlice(", ");
                }
                first = false;
                const key_json_prefix = try std.fmt.allocPrint(allocator, "\"{s}\": ", .{k});
                defer allocator.free(key_json_prefix);
                try out_buf.appendSlice(key_json_prefix);
                try snap.appendSlice(key_json_prefix);

                const child_key = try std.fmt.allocPrint(allocator, "{s}{c}{s}", .{ prefix, PATH_SEP, k });
                errdefer allocator.free(child_key);

                const vev = try sc.next();
                switch (vev) {
                    .object_start => {
                        try out_buf.append('{');
                        try snap.append('{');
                        var child_snap = std.array_list.Managed(u8).init(allocator);
                        defer child_snap.deinit();
                        try child_snap.append('{');
                        // Recurse: write into out_buf and child_snap simultaneously.
                        // We use a simple wrapper: collect into child_snap, then mirror to out_buf.
                        // But that loses simultaneity — instead recurse with out_buf + child_snap.
                        try collectNestedObjectDual(allocator, sc, rec, child_key, out_buf, &child_snap);
                        // out_buf already has the full child object appended (including '}').
                        // snap needs child_snap's content after the '{' we already wrote.
                        try snap.appendSlice(child_snap.items[1..]);
                        try rec.fields.put(allocator, child_key, .{ .raw = try child_snap.toOwnedSlice() });
                    },
                    .array_start => {
                        var arr_snap = std.array_list.Managed(u8).init(allocator);
                        errdefer arr_snap.deinit();
                        try arr_snap.append('[');
                        try collectRawArray(sc, &arr_snap);
                        try out_buf.appendSlice(arr_snap.items);
                        try snap.appendSlice(arr_snap.items);
                        try rec.fields.put(allocator, child_key, .{ .raw = try arr_snap.toOwnedSlice() });
                    },
                    .string => |s| {
                        const sv = try std.fmt.allocPrint(allocator, "\"{s}\"", .{s});
                        defer allocator.free(sv);
                        try out_buf.appendSlice(sv);
                        try snap.appendSlice(sv);
                        try rec.fields.put(allocator, child_key, .{ .string = try allocator.dupe(u8, s) });
                    },
                    .number => |n| {
                        var tmp: [32]u8 = undefined;
                        const ns = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch "0";
                        try out_buf.appendSlice(ns);
                        try snap.appendSlice(ns);
                        try rec.fields.put(allocator, child_key, .{ .number = n });
                    },
                    .boolean => |b| {
                        const bv = if (b) "true" else "false";
                        try out_buf.appendSlice(bv);
                        try snap.appendSlice(bv);
                        try rec.fields.put(allocator, child_key, .{ .boolean = b });
                    },
                    .null_val => {
                        try out_buf.appendSlice("null");
                        try snap.appendSlice("null");
                        try rec.fields.put(allocator, child_key, .null_val);
                    },
                    else => allocator.free(child_key),
                }
            },
            else => {},
        }
    }
}

/// Consume and append a JSON array (opening `[` already consumed, `[` already appended to raw_buf).
fn collectRawArray(sc: anytype, raw_buf: *std.array_list.Managed(u8)) !void {
    var depth: usize = 1;
    var first = true;
    while (depth > 0) {
        const inner = try sc.next();
        switch (inner) {
            .object_start => {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                try raw_buf.append('{');
                depth += 1;
            },
            .object_end => {
                depth -= 1;
                try raw_buf.append(if (depth > 0) '}' else '}');
            },
            .array_start => {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                try raw_buf.append('[');
                depth += 1;
            },
            .array_end => {
                depth -= 1;
                try raw_buf.append(']');
            },
            .key => |ik| {
                try raw_buf.append('"');
                try raw_buf.appendSlice(ik);
                try raw_buf.appendSlice("\": ");
                first = false;
            },
            .string => |s| {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                try raw_buf.append('"');
                try raw_buf.appendSlice(s);
                try raw_buf.append('"');
            },
            .number => |n| {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                var tmp: [32]u8 = undefined;
                const ns = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch "0";
                try raw_buf.appendSlice(ns);
            },
            .boolean => |b| {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                try raw_buf.appendSlice(if (b) "true" else "false");
            },
            .null_val => {
                if (!first) try raw_buf.appendSlice(", ");
                first = false;
                try raw_buf.appendSlice("null");
            },
            .end_of_input => return error.UnexpectedEof,
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

    const sel = if (q.fields) |f| @as(?[]const SelectField, f) else null;
    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;

    // For limit-only (no order_by): we can stop early
    const early_stop = q.limit != null and ob == null;
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
    array_loop: while (true) {
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
                const full_rec = try copyRecordFull(allocator, &rec);
                try matches.append(full_rec);

                if (early_stop and matches.items.len >= lim) break :array_loop;
            },
            else => {},
        }
    }

    // Sort on full records (dotted keys available)
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, matches.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    // Apply limit, then project and emit
    const count = @min(matches.items.len, lim);

    // Project into output records
    var out_recs = std.array_list.Managed(Record).init(allocator);
    defer {
        for (out_recs.items) |*r| r.deinit();
        out_recs.deinit();
    }
    for (matches.items[0..count]) |*rec| {
        const out_rec: Record = if (sel) |s|
            try projectRecord(allocator, rec, s)
        else
            try copyRecord(allocator, rec);
        try out_recs.append(out_rec);
    }

    try emitResults(out_recs.items, count, writer, opts.format, opts.raw);
    return count;
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
    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    const fmt = if (opts.raw) OutputFormat.raw else opts.format;

    const early_stop = q.limit != null and ob == null;
    const lim = q.limit orelse std.math.maxInt(usize);

    // Streaming emit: when there is no ORDER BY, no DISTINCT, no EXPAND, and the
    // output format doesn't require buffering (not CSV/TSV), emit each matching
    // record immediately from the scratch arena.  This eliminates copyRecordFull
    // and the matches[] heap vec entirely — O(1) memory for the common case.
    const has_expand = if (sel) |s| blk: {
        for (s) |f| {
            if (f.is_expand) break :blk true;
        }
        break :blk false;
    } else false;
    const streaming_emit = ob == null and !q.distinct and !has_expand and
        fmt != .csv and fmt != .tsv;
    var emit_count: usize = 0;
    var json_first = true; // for streaming JSON array formatting

    // Buffered path only: collect full records for ORDER BY / DISTINCT / EXPAND / CSV
    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Read line-by-line so we can access the raw bytes for --tee / --reject routing.
    // Each NDJSON record is exactly one line; we parse it with a fixed-slice sub-scanner.
    // For very long lines (> reader buffer), we fall back to a heap-allocated line_buf.
    var line_buf = std.array_list.Managed(u8).init(allocator);
    defer line_buf.deinit();

    ndjson_loop: while (true) {
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
        // Skip lines that don't start an object (arrays, bare values, comments)
        if (trimmed[0] != '{') {
            if (opts.tee_writer) |tw| {
                try tw.writeAll(trimmed);
                try tw.writeByte('\n');
            }
            continue :ndjson_loop;
        }

        // --tee: write every record (match or not) to the tee file
        if (opts.tee_writer) |tw| {
            try tw.writeAll(trimmed);
            try tw.writeByte('\n');
        }

        // ── Raw-byte fast path ────────────────────────────────────────────────
        // Attempt WHERE evaluation directly on raw bytes before any parsing.
        // If the WHERE is satisfied AND we only need SELECT * (no projection,
        // no ORDER BY, no DISTINCT) in streaming-emit mode, emit the raw line
        // directly — zero allocations, zero parse overhead per record.
        if (q.where) |w| {
            if (tryWhereOnRaw(trimmed, w)) |passes| {
                if (!passes) {
                    if (opts.reject_writer) |rw| {
                        try rw.writeAll(trimmed);
                        try rw.writeByte('\n');
                    }
                    continue :ndjson_loop;
                }
                // WHERE passed on raw bytes — if SELECT * and streaming emit,
                // emit the raw line directly without any parsing.
                if (streaming_emit and sel == null) {
                    switch (fmt) {
                        .json => {
                            if (json_first) {
                                try writer.writeAll("[\n  ");
                                json_first = false;
                            } else try writer.writeAll(",\n  ");
                            try writer.writeAll(trimmed);
                        },
                        .ndjson, .raw => {
                            try writer.writeAll(trimmed);
                            try writer.writeByte('\n');
                            try writer.flush();
                        },
                        .csv, .tsv => unreachable,
                    }
                    emit_count += 1;
                    if (early_stop and emit_count >= lim) break :ndjson_loop;
                    continue :ndjson_loop;
                }
                // WHERE passed but we still need projection/ordering — fall through
                // to full parse, but skip the WHERE re-check below.
                _ = scratch.reset(.retain_capacity);
                var line_reader_fp = std.Io.Reader.fixed(trimmed);
                var sub_sc_fp = stream.Scanner.init(scratch.allocator(), &line_reader_fp) catch {
                    if (opts.skipped_records) |sk| sk.* += 1;
                    continue :ndjson_loop;
                };
                const first_ev_fp = sub_sc_fp.next() catch {
                    if (opts.skipped_records) |sk| sk.* += 1;
                    continue :ndjson_loop;
                };
                if (first_ev_fp != .object_start) {
                    if (opts.skipped_records) |sk| sk.* += 1;
                    continue :ndjson_loop;
                }
                var rec_fp = collectRecord(scratch.allocator(), &sub_sc_fp) catch {
                    if (opts.skipped_records) |sk| sk.* += 1;
                    continue :ndjson_loop;
                };
                // Skip to emit — WHERE already confirmed above
                if (streaming_emit) {
                    var out_rec_fp = try projectRecord(scratch.allocator(), &rec_fp, sel.?);
                    try emitStreamingRecord(writer, &out_rec_fp, fmt, &json_first);
                    emit_count += 1;
                    if (early_stop and emit_count >= lim) break :ndjson_loop;
                } else {
                    const full_rec_fp = try copyRecordFull(allocator, &rec_fp);
                    try matches.append(full_rec_fp);
                    if (early_stop and matches.items.len >= lim) break :ndjson_loop;
                }
                continue :ndjson_loop;
            }
            // null → raw evaluation inconclusive, fall through to full parse
        }

        _ = scratch.reset(.retain_capacity);
        var line_reader = std.Io.Reader.fixed(trimmed);
        var sub_sc = stream.Scanner.init(scratch.allocator(), &line_reader) catch {
            if (opts.skipped_records) |sk| sk.* += 1;
            continue :ndjson_loop;
        };
        // consume the opening `{` (object_start event)
        const first_ev = sub_sc.next() catch {
            if (opts.skipped_records) |sk| sk.* += 1;
            continue :ndjson_loop;
        };
        if (first_ev != .object_start) {
            if (opts.skipped_records) |sk| sk.* += 1;
            continue :ndjson_loop;
        }

        var rec = collectRecord(scratch.allocator(), &sub_sc) catch {
            if (opts.skipped_records) |sk| sk.* += 1;
            continue :ndjson_loop;
        };

        if (q.where) |w| {
            if (!recordPassesWhere(scratch.allocator(), &rec, w)) {
                // --reject: write non-matching records to the reject file
                if (opts.reject_writer) |rw| {
                    try rw.writeAll(trimmed);
                    try rw.writeByte('\n');
                }
                continue :ndjson_loop;
            }
        }

        // ── Streaming emit path ───────────────────────────────────────────────
        // Emit immediately from scratch arena — no heap allocation, no copyRecordFull.
        if (streaming_emit) {
            if (sel) |fields| {
                // Project into scratch arena (all allocs freed on next scratch.reset())
                var out_rec = try projectRecord(scratch.allocator(), &rec, fields);
                switch (fmt) {
                    .json => {
                        if (json_first) {
                            try writer.writeAll("[\n  ");
                            json_first = false;
                        } else try writer.writeAll(",\n  ");
                        try writeRecordJson(writer, &out_rec);
                    },
                    .ndjson => {
                        try writeRecordJson(writer, &out_rec);
                        try writer.writeByte('\n');
                        try writer.flush();
                    },
                    .raw => {
                        var it = out_rec.fields.iterator();
                        if (it.next()) |e| try writeOwnedValueRaw(writer, e.value_ptr.*);
                        try writer.writeByte('\n');
                        try writer.flush();
                    },
                    .csv, .tsv => unreachable,
                }
            } else {
                // No field list (SELECT * or bare passthrough): emit the raw trimmed line
                switch (fmt) {
                    .json => {
                        if (json_first) {
                            try writer.writeAll("[\n  ");
                            json_first = false;
                        } else try writer.writeAll(",\n  ");
                        try writer.writeAll(trimmed);
                    },
                    .ndjson, .raw => {
                        try writer.writeAll(trimmed);
                        try writer.writeByte('\n');
                        try writer.flush();
                    },
                    .csv, .tsv => unreachable,
                }
            }
            emit_count += 1;
            if (early_stop and emit_count >= lim) break :ndjson_loop;
            continue :ndjson_loop;
        }

        // ── Buffered path (ORDER BY / DISTINCT / EXPAND / CSV) ───────────────
        const full_rec = try copyRecordFull(allocator, &rec);
        try matches.append(full_rec);

        if (early_stop and matches.items.len >= lim) break :ndjson_loop;
    }

    // Close streaming JSON array and return
    if (streaming_emit) {
        switch (fmt) {
            .json => if (emit_count == 0)
                try writer.writeAll("[]\n")
            else
                try writer.writeAll("\n]\n"),
            .ndjson, .raw => {},
            .csv, .tsv => unreachable,
        }
        return emit_count;
    }

    // Sort on full records (dotted keys available for ORDER BY)
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, matches.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const count = @min(matches.items.len, lim);

    // Find any EXPAND field in the select list
    const expand_field: ?SelectField = if (sel) |s| blk: {
        for (s) |f| {
            if (f.is_expand) break :blk f;
        }
        break :blk null;
    } else null;

    // Project into output records
    var out_recs = std.array_list.Managed(Record).init(allocator);
    defer {
        for (out_recs.items) |*r| r.deinit();
        out_recs.deinit();
    }

    // DISTINCT dedup set: keyed by serialised field values
    var distinct_set = std.StringHashMap(void).init(allocator);
    defer {
        var it_ds = distinct_set.keyIterator();
        while (it_ds.next()) |k| allocator.free(k.*);
        distinct_set.deinit();
    }

    for (matches.items[0..count]) |*rec| {
        // EXPAND: expand an array field into multiple output rows
        if (expand_field) |ef| {
            // Determine the JSON array to expand.
            // For expand(split(field, delim)), apply split first.
            var split_arena: ?std.heap.ArenaAllocator = null;
            defer if (split_arena) |*a| a.deinit();
            const arr_json: []const u8 = if (ef.expand_split_delim) |delim| blk: {
                const field_ov = rec.get(ef.key) orelse continue;
                const field_str: []const u8 = switch (field_ov) {
                    .string => |s| s,
                    .raw => |r| r,
                    else => continue,
                };
                // Build a JSON array by splitting field_str on delim
                split_arena = std.heap.ArenaAllocator.init(allocator);
                const sa = split_arena.?.allocator();
                var arr_buf = std.array_list.Managed(u8).init(sa);
                try arr_buf.append('[');
                var first_elem = true;
                var it_sp = std.mem.splitSequence(u8, field_str, delim);
                while (it_sp.next()) |tok_str| {
                    if (!first_elem) try arr_buf.append(',');
                    first_elem = false;
                    try arr_buf.append('"');
                    for (tok_str) |c| {
                        if (c == '"') try arr_buf.appendSlice("\\\"") else if (c == '\\') try arr_buf.appendSlice("\\\\") else try arr_buf.append(c);
                    }
                    try arr_buf.append('"');
                }
                try arr_buf.append(']');
                break :blk arr_buf.items;
            } else blk: {
                const arr_ov = rec.get(ef.key) orelse continue;
                // The array is stored as a .raw JSON string — parse and iterate
                break :blk switch (arr_ov) {
                    .raw => |r| r,
                    .string => |s| s,
                    else => continue,
                };
            };
            // Parse the JSON array
            var parsed = std.json.parseFromSlice(std.json.Value, allocator, arr_json, .{}) catch continue;
            defer parsed.deinit();
            if (parsed.value != .array) continue;
            for (parsed.value.array.items) |elem| {
                // Project non-expand fields from the current record, then add the element
                var out_rec: Record = if (sel) |s|
                    try projectRecord(allocator, rec, s)
                else
                    try copyRecord(allocator, rec);
                errdefer out_rec.deinit();
                const exp_key = try allocator.dupe(u8, if (ef.alias) |a| a else ef.key);
                errdefer allocator.free(exp_key);
                const exp_val: OwnedValue = switch (elem) {
                    .string => |s| .{ .string = try allocator.dupe(u8, s) },
                    .integer => |n| .{ .number = @floatFromInt(n) },
                    .float => |f2| .{ .number = f2 },
                    .bool => |b| .{ .boolean = b },
                    .null => .null_val,
                    else => v: {
                        var aw = std.Io.Writer.Allocating.init(allocator);
                        errdefer aw.deinit();
                        try std.json.Stringify.value(elem, .{}, &aw.writer);
                        break :v OwnedValue{ .raw = try aw.toOwnedSlice() };
                    },
                };
                try out_rec.fields.put(allocator, exp_key, exp_val);
                try out_recs.append(out_rec);
            }
            continue;
        }

        // Normal projection
        var out_rec: Record = if (sel) |s|
            try projectRecord(allocator, rec, s)
        else
            try copyRecord(allocator, rec);
        errdefer out_rec.deinit();

        // DISTINCT: compute a fingerprint of all field values and skip duplicates
        if (q.distinct) {
            var fp_buf = std.array_list.Managed(u8).init(allocator);
            defer fp_buf.deinit();
            var it2 = out_rec.fields.iterator();
            while (it2.next()) |e| {
                try fp_buf.appendSlice(e.key_ptr.*);
                try fp_buf.append('=');
                switch (e.value_ptr.*) {
                    .string => |s| try fp_buf.appendSlice(s),
                    .number => |n| {
                        var tb: [32]u8 = undefined;
                        const ns = std.fmt.bufPrint(&tb, "{d}", .{n}) catch "";
                        try fp_buf.appendSlice(ns);
                    },
                    .boolean => |b| try fp_buf.appendSlice(if (b) "true" else "false"),
                    .null_val => try fp_buf.appendSlice("null"),
                    .raw => |r| try fp_buf.appendSlice(r),
                }
                try fp_buf.append(';');
            }
            const fp = fp_buf.items;
            if (distinct_set.contains(fp)) {
                out_rec.deinit();
                continue;
            }
            const fp_owned = try allocator.dupe(u8, fp);
            try distinct_set.put(fp_owned, {});
        }

        try out_recs.append(out_rec);
    }

    const out_count = out_recs.items.len;
    try emitResults(out_recs.items, out_count, writer, opts.format, opts.raw);
    return out_count;
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

/// Internal group-by aggregation over an already-collected slice of Records.
/// Returns a new slice of Records (one per group), owned by `allocator`.
fn aggregateGroupBy(
    allocator: std.mem.Allocator,
    records_iter: anytype, // must support: while(try iter.next()) |rec| { ... }
    group_keys: []const query.GroupByKey,
    count_alias: []const u8, // the output field name for the count
    extra_fields: ?[]const SelectField, // non-count SELECT fields (typically just the group key)
    where: ?query.WhereClause,
) !std.array_list.Managed(Record) {
    const n_group_keys = group_keys.len;
    // Per-group state
    const AggState = struct {
        count: usize,
        group_vals: []OwnedValue, // one per group key, heap-owned slice
        // For each aggregate field (sum/avg/min/max/stddev/variance): running accumulators.
        // We store parallel slices indexed by position in extra_fields agg list.
        // stddev/variance use Welford's online algorithm (means + m2s).
        sums: []f64,
        mins: []f64,
        maxs: []f64,
        means: []f64, // Welford running mean
        m2s: []f64, // Welford sum of squared deviations
        n_numeric: []usize, // count of numeric contributions per agg field
        allocator: std.mem.Allocator,
        n_aggs: usize,

        fn deinit(self: *@This()) void {
            for (self.group_vals) |*gv| gv.deinit(self.allocator);
            self.allocator.free(self.group_vals);
            self.allocator.free(self.sums);
            self.allocator.free(self.mins);
            self.allocator.free(self.maxs);
            self.allocator.free(self.means);
            self.allocator.free(self.m2s);
            self.allocator.free(self.n_numeric);
        }
    };

    // Count how many agg fields there are
    var n_aggs: usize = 0;
    if (extra_fields) |ef| for (ef) |f| {
        if (f.agg != null) n_aggs += 1;
    };

    var states = std.StringHashMap(AggState).init(allocator);
    defer {
        var sit = states.iterator();
        while (sit.next()) |e| {
            allocator.free(e.key_ptr.*);
            e.value_ptr.deinit();
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

        // Build composite group key from all group-by fields, joined by \x00.
        // If any key field is missing (and no isnull default), skip the record.
        var composite_buf = std.array_list.Managed(u8).init(scratch.allocator());
        var gv_list: [16]OwnedValue = undefined; // stack buffer, supports up to 16 group keys
        var gv_count: usize = 0;
        var skip_record = false;

        for (group_keys) |gk| {
            if (gv_count >= gv_list.len) break;
            // Evaluate: if the group key has an expression, eval it; otherwise field lookup.
            const gv_raw: ?OwnedValue = if (gk.expr) |expr|
                evalExpr(scratch.allocator(), &r, expr)
            else
                r.get(gk.key);
            // isnull default lookup (by output key name)
            const isnull_default: ?[]const u8 = if (extra_fields) |ef| blk: {
                for (ef) |f| {
                    const fk = f.key;
                    const matches = std.mem.eql(u8, fk, gk.key) or
                        (f.alias != null and std.mem.eql(u8, f.alias.?, gk.key));
                    if (matches and f.default_val != null) break :blk f.default_val;
                }
                break :blk null;
            } else null;

            const gv: OwnedValue = if (gv_raw) |v| blk: {
                if (v == .null_val) {
                    if (isnull_default) |def| break :blk OwnedValue{ .string = @constCast(def) };
                }
                break :blk v;
            } else if (isnull_default) |def|
                OwnedValue{ .string = @constCast(def) }
            else blk: {
                skip_record = true;
                break :blk OwnedValue.null_val;
            };

            if (skip_record) break;

            // Append this key's string representation to the composite key
            if (gv_count > 0) try composite_buf.append(0); // null byte separator
            var part_buf: [256]u8 = undefined;
            const part: []const u8 = switch (gv) {
                .string => |s| s,
                .number => |n| std.fmt.bufPrint(&part_buf, "{d}", .{n}) catch continue,
                .boolean => |b| if (b) "true" else "false",
                .null_val => "null",
                .raw => |rv| rv,
            };
            try composite_buf.appendSlice(part);
            gv_list[gv_count] = gv;
            gv_count += 1;
        }

        if (skip_record or gv_count != n_group_keys) continue;

        const key_str = composite_buf.items;
        const gop = try states.getOrPut(key_str);
        if (!gop.found_existing) {
            gop.key_ptr.* = try allocator.dupe(u8, key_str);
            // Own copies of each group value
            const owned_gvs = try allocator.alloc(OwnedValue, n_group_keys);
            for (gv_list[0..gv_count], 0..) |gv, gi| {
                owned_gvs[gi] = try gv.copy(allocator);
            }
            const sums = try allocator.alloc(f64, n_aggs);
            @memset(sums, 0.0);
            const mins = try allocator.alloc(f64, n_aggs);
            @memset(mins, std.math.floatMax(f64));
            const maxs = try allocator.alloc(f64, n_aggs);
            @memset(maxs, -std.math.floatMax(f64));
            const means = try allocator.alloc(f64, n_aggs);
            @memset(means, 0.0);
            const m2s = try allocator.alloc(f64, n_aggs);
            @memset(m2s, 0.0);
            const n_numeric = try allocator.alloc(usize, n_aggs);
            @memset(n_numeric, 0);
            gop.value_ptr.* = AggState{
                .count = 0,
                .group_vals = owned_gvs,
                .sums = sums,
                .mins = mins,
                .maxs = maxs,
                .means = means,
                .m2s = m2s,
                .n_numeric = n_numeric,
                .allocator = allocator,
                .n_aggs = n_aggs,
            };
        }
        gop.value_ptr.count += 1;

        // Accumulate aggregate fields
        if (extra_fields) |ef| {
            var agg_idx: usize = 0;
            for (ef) |f| {
                if (f.agg == null) continue;
                defer agg_idx += 1;
                const fv = r.get(f.key) orelse continue;
                const n = toF64(fv) orelse continue;
                gop.value_ptr.sums[agg_idx] += n;
                if (n < gop.value_ptr.mins[agg_idx]) gop.value_ptr.mins[agg_idx] = n;
                if (n > gop.value_ptr.maxs[agg_idx]) gop.value_ptr.maxs[agg_idx] = n;
                gop.value_ptr.n_numeric[agg_idx] += 1;
                // Welford online update for variance/stddev
                const cnt_f: f64 = @floatFromInt(gop.value_ptr.n_numeric[agg_idx]);
                const delta = n - gop.value_ptr.means[agg_idx];
                gop.value_ptr.means[agg_idx] += delta / cnt_f;
                const delta2 = n - gop.value_ptr.means[agg_idx];
                gop.value_ptr.m2s[agg_idx] += delta * delta2;
            }
        }
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

        // Add count field (only if count() was in the SELECT)
        if (count_alias.len > 0) {
            const cnt_name = try allocator.dupe(u8, count_alias);
            errdefer allocator.free(cnt_name);
            try rec.fields.put(allocator, cnt_name, .{ .number = @floatFromInt(state.count) });
        }

        // Add aggregate fields
        if (extra_fields) |ef| {
            var agg_idx: usize = 0;
            for (ef) |f| {
                if (f.agg == null) continue;
                defer agg_idx += 1;
                const out_name = try allocator.dupe(u8, if (f.alias) |a| a else f.key);
                const n_seen = state.n_numeric[agg_idx];
                const agg_val: OwnedValue = if (n_seen == 0) .null_val else switch (f.agg.?) {
                    .sum => .{ .number = state.sums[agg_idx] },
                    .avg => .{ .number = state.sums[agg_idx] / @as(f64, @floatFromInt(n_seen)) },
                    .min => .{ .number = state.mins[agg_idx] },
                    .max => .{ .number = state.maxs[agg_idx] },
                    .variance => .{ .number = state.m2s[agg_idx] / @as(f64, @floatFromInt(n_seen)) },
                    .stddev => .{ .number = @sqrt(state.m2s[agg_idx] / @as(f64, @floatFromInt(n_seen))) },
                };
                try rec.fields.put(allocator, out_name, agg_val);
            }
        }

        try result.append(rec);
    }

    return result;
}

/// Iterator wrapper around a Scanner reading a NDJSON stream
const NdjsonIter = struct {
    sc: *stream.Scanner,
    scratch_arena: *std.heap.ArenaAllocator,

    pub fn next(self: *NdjsonIter) !?Record {
        while (true) {
            const ev = self.sc.next() catch |err| switch (err) {
                error.EndOfStream => return null,
                else => return err,
            };
            switch (ev) {
                .end_of_input => return null,
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

/// Resolve the count alias from the SELECT field list (the SelectField with key=="count").
fn resolveCountAlias(fields: ?[]const SelectField) []const u8 {
    if (fields) |flds| {
        for (flds) |f| {
            if (std.mem.eql(u8, f.key, "count") and f.agg == null) {
                if (f.alias) |a| return a;
                return "count";
            }
        }
    }
    return ""; // empty = no count field requested
}

/// Execute a group-by query on NDJSON input.
pub fn execGroupByNDJSON(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    if (q.group_by == null) return error.NoGroupByKey;
    const count_alias = resolveCountAlias(q.fields);

    var sc = try stream.Scanner.init(allocator, reader);
    defer sc.deinit();
    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    var iter = NdjsonIter{ .sc = &sc, .scratch_arena = &scratch };
    var groups = try aggregateGroupBy(allocator, &iter, q.group_by.?, count_alias, q.fields, q.where);
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }

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

    // Sort if requested
    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, groups.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const lim = q.limit orelse std.math.maxInt(usize);
    const count = @min(groups.items.len, lim);
    try emitResults(groups.items, count, writer, opts.format, opts.raw);
    return count;
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
    const count_alias = resolveCountAlias(q.fields);

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
    var groups = try aggregateGroupBy(allocator, &iter, q.group_by.?, count_alias, q.fields, q.where);
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }

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

    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, groups.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const lim = q.limit orelse std.math.maxInt(usize);
    const count = @min(groups.items.len, lim);
    try emitResults(groups.items, count, writer, opts.format, opts.raw);
    return count;
}

// ─── Global aggregate executor ───────────────────────────────────────────────

pub fn execGlobalAggNDJSON(
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

    const fields = q.fields orelse return 0;
    var n_aggs: usize = 0;
    for (fields) |f| {
        if (f.agg != null) n_aggs += 1;
    }

    const sums = try allocator.alloc(f64, n_aggs);
    defer allocator.free(sums);
    const mins = try allocator.alloc(f64, n_aggs);
    defer allocator.free(mins);
    const maxs = try allocator.alloc(f64, n_aggs);
    defer allocator.free(maxs);
    const means = try allocator.alloc(f64, n_aggs);
    defer allocator.free(means);
    const m2s = try allocator.alloc(f64, n_aggs);
    defer allocator.free(m2s);
    const n_num = try allocator.alloc(usize, n_aggs);
    defer allocator.free(n_num);
    @memset(sums, 0.0);
    @memset(mins, std.math.floatMax(f64));
    @memset(maxs, -std.math.floatMax(f64));
    @memset(means, 0.0);
    @memset(m2s, 0.0);
    @memset(n_num, 0);
    var total_count: usize = 0;

    while (true) {
        const ev = sc.next() catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        switch (ev) {
            .end_of_input => break,
            .object_start => {
                _ = scratch.reset(.retain_capacity);
                var rec = try collectRecord(scratch.allocator(), &sc);
                if (q.where) |w| {
                    if (!recordPassesWhere(scratch.allocator(), &rec, w)) continue;
                }
                total_count += 1;
                var agg_idx: usize = 0;
                for (fields) |f| {
                    if (f.agg == null) continue;
                    defer agg_idx += 1;
                    const val = rec.get(f.key) orelse continue;
                    const nv: f64 = switch (val) {
                        .number => |x| x,
                        .string => |s| std.fmt.parseFloat(f64, s) catch continue,
                        else => continue,
                    };
                    sums[agg_idx] += nv;
                    if (nv < mins[agg_idx]) mins[agg_idx] = nv;
                    if (nv > maxs[agg_idx]) maxs[agg_idx] = nv;
                    n_num[agg_idx] += 1;
                    const cnt_f: f64 = @floatFromInt(n_num[agg_idx]);
                    const delta = nv - means[agg_idx];
                    means[agg_idx] += delta / cnt_f;
                    m2s[agg_idx] += delta * (nv - means[agg_idx]);
                }
            },
            else => {},
        }
    }

    var out_rec = Record.init(allocator);
    defer out_rec.deinit();
    const count_alias = resolveCountAlias(q.fields);
    if (count_alias.len > 0) {
        try out_rec.fields.put(allocator, try allocator.dupe(u8, count_alias), .{ .number = @floatFromInt(total_count) });
    }
    var agg_idx: usize = 0;
    for (fields) |f| {
        if (f.agg == null) continue;
        defer agg_idx += 1;
        const agg_val: OwnedValue = if (n_num[agg_idx] == 0) .null_val else switch (f.agg.?) {
            .sum => .{ .number = sums[agg_idx] },
            .avg => .{ .number = sums[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .min => .{ .number = mins[agg_idx] },
            .max => .{ .number = maxs[agg_idx] },
            .variance => .{ .number = m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .stddev => .{ .number = @sqrt(m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx]))) },
        };
        try out_rec.fields.put(allocator, try allocator.dupe(u8, if (f.alias) |a| a else f.key), agg_val);
    }
    var one_rec = [1]Record{out_rec};
    try emitResults(&one_rec, 1, writer, opts.format, opts.raw);
    return 1;
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
//   curl ... | kq --llm response 'select make, model where type = "sedan"'

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
    const early_stop = q.limit != null and ob == null;
    const lim = q.limit orelse std.math.maxInt(usize);
    const fmt = if (opts.raw) OutputFormat.raw else opts.format;

    // When no ORDER BY and no DISTINCT, emit each matched record immediately
    // to the writer as it arrives rather than buffering for post-processing.
    const streaming_emit = ob == null and !q.distinct and fmt != .csv and fmt != .tsv;
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
        std.debug.print("kq: warning: stream ended with {d} bytes of incomplete JSON discarded\n", .{accum.items.len});
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

    // Post-process: sort, limit, project, emit (same as execStreamNDJSON)
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, matches.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const count = @min(matches.items.len, lim);

    var out_recs = std.array_list.Managed(Record).init(allocator);
    defer {
        for (out_recs.items) |*r| r.deinit();
        out_recs.deinit();
    }

    // DISTINCT dedup set
    var distinct_set = std.StringHashMap(void).init(allocator);
    defer {
        var it_ds = distinct_set.keyIterator();
        while (it_ds.next()) |k| allocator.free(k.*);
        distinct_set.deinit();
    }

    for (matches.items[0..count]) |*rec| {
        var out_rec: Record = if (sel) |s|
            try projectRecord(allocator, rec, s)
        else
            try copyRecord(allocator, rec);
        errdefer out_rec.deinit();

        if (q.distinct) {
            var fp_buf = std.array_list.Managed(u8).init(allocator);
            defer fp_buf.deinit();
            var it2 = out_rec.fields.iterator();
            while (it2.next()) |e| {
                try fp_buf.appendSlice(e.key_ptr.*);
                try fp_buf.append('=');
                switch (e.value_ptr.*) {
                    .string => |s| try fp_buf.appendSlice(s),
                    .number => |n| {
                        var tb: [32]u8 = undefined;
                        const ns = std.fmt.bufPrint(&tb, "{d}", .{n}) catch "";
                        try fp_buf.appendSlice(ns);
                    },
                    .boolean => |b| try fp_buf.appendSlice(if (b) "true" else "false"),
                    .null_val => try fp_buf.appendSlice("null"),
                    .raw => |r| try fp_buf.appendSlice(r),
                }
                try fp_buf.append(';');
            }
            const fp = fp_buf.items;
            if (distinct_set.contains(fp)) {
                out_rec.deinit();
                continue;
            }
            const fp_owned = try allocator.dupe(u8, fp);
            try distinct_set.put(fp_owned, {});
        }

        try out_recs.append(out_rec);
    }

    const out_count = out_recs.items.len;
    try emitResults(out_recs.items, out_count, writer, opts.format, opts.raw);
    return out_count;
}

/// LLM global aggregate mode — accumulates token fragments into JSON objects,
/// then runs streaming aggregation (count/sum/avg/min/max) across all extracted objects.
pub fn execLlmGlobalAgg(
    allocator: std.mem.Allocator,
    reader: *std.Io.Reader,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const use_sse = opts.api_mode != null and opts.api_mode.? != .ollama;
    const fields = q.fields orelse return 0;

    var n_aggs: usize = 0;
    for (fields) |f| {
        if (f.agg != null) n_aggs += 1;
    }

    const sums = try allocator.alloc(f64, n_aggs);
    defer allocator.free(sums);
    const mins = try allocator.alloc(f64, n_aggs);
    defer allocator.free(mins);
    const maxs = try allocator.alloc(f64, n_aggs);
    defer allocator.free(maxs);
    const means = try allocator.alloc(f64, n_aggs);
    defer allocator.free(means);
    const m2s = try allocator.alloc(f64, n_aggs);
    defer allocator.free(m2s);
    const n_num = try allocator.alloc(usize, n_aggs);
    defer allocator.free(n_num);
    @memset(sums, 0.0);
    @memset(mins, std.math.floatMax(f64));
    @memset(maxs, -std.math.floatMax(f64));
    @memset(means, 0.0);
    @memset(m2s, 0.0);
    @memset(n_num, 0);
    var total_count: usize = 0;

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

            if (q.where) |w| {
                if (!recordPassesWhere(scratch.allocator(), &rec, w)) continue;
            }

            total_count += 1;
            var agg_idx: usize = 0;
            for (fields) |f| {
                if (f.agg == null) continue;
                defer agg_idx += 1;
                const val = rec.get(f.key) orelse continue;
                const nv: f64 = switch (val) {
                    .number => |x| x,
                    .string => |s| std.fmt.parseFloat(f64, s) catch continue,
                    else => continue,
                };
                sums[agg_idx] += nv;
                if (nv < mins[agg_idx]) mins[agg_idx] = nv;
                if (nv > maxs[agg_idx]) maxs[agg_idx] = nv;
                n_num[agg_idx] += 1;
                const cnt_f_llm: f64 = @floatFromInt(n_num[agg_idx]);
                const delta_llm = nv - means[agg_idx];
                means[agg_idx] += delta_llm / cnt_f_llm;
                m2s[agg_idx] += delta_llm * (nv - means[agg_idx]);
            }

            // --rolling: emit a snapshot of the running aggregates after each object
            if (opts.rolling) {
                try emitAggSnapshot(writer, fields, total_count, sums, mins, maxs, m2s, n_num, resolveCountAlias(q.fields));
                try writer.flush();
            }
        }
    }

    var out_rec = Record.init(allocator);
    defer out_rec.deinit();
    const count_alias = resolveCountAlias(q.fields);
    if (count_alias.len > 0) {
        try out_rec.fields.put(allocator, try allocator.dupe(u8, count_alias), .{ .number = @floatFromInt(total_count) });
    }
    var agg_idx: usize = 0;
    for (fields) |f| {
        if (f.agg == null) continue;
        defer agg_idx += 1;
        const agg_val: OwnedValue = if (n_num[agg_idx] == 0) .null_val else switch (f.agg.?) {
            .sum => .{ .number = sums[agg_idx] },
            .avg => .{ .number = sums[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .min => .{ .number = mins[agg_idx] },
            .max => .{ .number = maxs[agg_idx] },
            .variance => .{ .number = m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .stddev => .{ .number = @sqrt(m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx]))) },
        };
        try out_rec.fields.put(allocator, try allocator.dupe(u8, if (f.alias) |a| a else f.key), agg_val);
    }
    var one_rec = [1]Record{out_rec};
    if (!opts.rolling) {
        try emitResults(&one_rec, 1, writer, opts.format, opts.raw);
    }
    return 1;
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
    const count_alias = resolveCountAlias(q.fields);

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
                var snap_groups = try aggregateGroupBy(allocator, &snap_iter, q.group_by.?, count_alias, q.fields, null);
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
    var groups = try aggregateGroupBy(allocator, &iter, q.group_by.?, count_alias, q.fields, null);
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }

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

    const ob_fields = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    if (ob_fields) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, groups.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const final_lim = q.limit orelse std.math.maxInt(usize);
    const final_count = @min(groups.items.len, final_lim);
    if (!opts.rolling) {
        try emitResults(groups.items, final_count, writer, opts.format, opts.raw);
    }
    return final_count;
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
    const sel = if (q.fields) |f| @as(?[]const SelectField, f) else null;
    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    const early_stop = q.limit != null and ob == null;
    const lim = q.limit orelse std.math.maxInt(usize);

    var matches = std.array_list.Managed(Record).init(allocator);
    defer {
        for (matches.items) |*r| r.deinit();
        matches.deinit();
    }

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    // Consume records from the source
    while (try source.next()) |rec_val| {
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

        if (early_stop and matches.items.len >= lim) break;
    }

    // Sort
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, matches.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const count = @min(matches.items.len, lim);

    // Project into output records
    var out_recs = std.array_list.Managed(Record).init(allocator);
    defer {
        for (out_recs.items) |*r| r.deinit();
        out_recs.deinit();
    }

    // DISTINCT dedup set
    var distinct_set = std.StringHashMap(void).init(allocator);
    defer {
        var it_ds = distinct_set.keyIterator();
        while (it_ds.next()) |k| allocator.free(k.*);
        distinct_set.deinit();
    }

    for (matches.items[0..count]) |*rec| {
        var out_rec: Record = if (sel) |s|
            try projectRecord(allocator, rec, s)
        else
            try copyRecord(allocator, rec);
        errdefer out_rec.deinit();

        if (q.distinct) {
            var fp_buf = std.array_list.Managed(u8).init(allocator);
            defer fp_buf.deinit();
            var it2 = out_rec.fields.iterator();
            while (it2.next()) |e| {
                try fp_buf.appendSlice(e.key_ptr.*);
                try fp_buf.append('=');
                switch (e.value_ptr.*) {
                    .string => |s| try fp_buf.appendSlice(s),
                    .number => |n| {
                        var tb: [32]u8 = undefined;
                        const ns = std.fmt.bufPrint(&tb, "{d}", .{n}) catch "";
                        try fp_buf.appendSlice(ns);
                    },
                    .boolean => |b| try fp_buf.appendSlice(if (b) "true" else "false"),
                    .null_val => try fp_buf.appendSlice("null"),
                    .raw => |r| try fp_buf.appendSlice(r),
                }
                try fp_buf.append(';');
            }
            const fp = fp_buf.items;
            if (distinct_set.contains(fp)) {
                out_rec.deinit();
                continue;
            }
            const fp_owned = try allocator.dupe(u8, fp);
            try distinct_set.put(fp_owned, {});
        }

        try out_recs.append(out_rec);
    }

    const out_count = out_recs.items.len;
    try emitResults(out_recs.items, out_count, writer, opts.format, opts.raw);
    return out_count;
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
    const count_alias = resolveCountAlias(q.fields);

    // Wrap source in a deiniting iterator that frees each record after the
    // next one is requested. aggregateGroupBy was designed for arena-backed
    // iterators (NdjsonIter/ScopedIter) where records are bulk-freed by the
    // arena reset. Generic sources allocate records on the heap, so we must
    // deinit them after aggregateGroupBy has extracted the needed values.
    var deinit_iter = DeinitIter(@TypeOf(source.*)){
        .inner = source,
        .prev = null,
    };
    var groups = try aggregateGroupBy(allocator, &deinit_iter, q.group_by.?, count_alias, q.fields, q.where);
    // Deinit the last record if any
    if (deinit_iter.prev) |*p| p.deinit();
    defer {
        for (groups.items) |*r| r.deinit();
        groups.deinit();
    }

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

    const ob = if (q.order_by) |o| @as(?[]const OrderField, o) else null;
    if (ob) |order| {
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: Record, b: Record) bool {
                return compareRecords(ctx.ob, &a, &b) == .lt;
            }
        };
        std.sort.block(Record, groups.items, Ctx{ .ob = order }, Ctx.lessThan);
    }

    const lim = q.limit orelse std.math.maxInt(usize);
    const count = @min(groups.items.len, lim);
    try emitResults(groups.items, count, writer, opts.format, opts.raw);
    return count;
}

/// Generic global aggregate executor. Works with any record source.
pub fn execGenericGlobalAgg(
    allocator: std.mem.Allocator,
    source: anytype,
    q: query.Query,
    writer: *std.Io.Writer,
    opts: ExecOptions,
) !usize {
    const fields = q.fields orelse return 0;
    var n_aggs: usize = 0;
    for (fields) |f| {
        if (f.agg != null) n_aggs += 1;
    }

    const sums = try allocator.alloc(f64, n_aggs);
    defer allocator.free(sums);
    const mins = try allocator.alloc(f64, n_aggs);
    defer allocator.free(mins);
    const maxs = try allocator.alloc(f64, n_aggs);
    defer allocator.free(maxs);
    const means = try allocator.alloc(f64, n_aggs);
    defer allocator.free(means);
    const m2s = try allocator.alloc(f64, n_aggs);
    defer allocator.free(m2s);
    const n_num = try allocator.alloc(usize, n_aggs);
    defer allocator.free(n_num);
    @memset(sums, 0.0);
    @memset(mins, std.math.floatMax(f64));
    @memset(maxs, -std.math.floatMax(f64));
    @memset(means, 0.0);
    @memset(m2s, 0.0);
    @memset(n_num, 0);
    var total_count: usize = 0;

    var scratch = std.heap.ArenaAllocator.init(allocator);
    defer scratch.deinit();

    while (try source.next()) |rec_val| {
        _ = scratch.reset(.retain_capacity);
        var rec = rec_val;
        defer rec.deinit();

        if (q.where) |w| {
            if (!recordPassesWhere(scratch.allocator(), &rec, w)) continue;
        }

        total_count += 1;
        var agg_idx: usize = 0;
        for (fields) |f| {
            if (f.agg == null) continue;
            defer agg_idx += 1;
            const val = rec.get(f.key) orelse continue;
            const nv: f64 = switch (val) {
                .number => |x| x,
                .string => |s| std.fmt.parseFloat(f64, s) catch continue,
                else => continue,
            };
            sums[agg_idx] += nv;
            if (nv < mins[agg_idx]) mins[agg_idx] = nv;
            if (nv > maxs[agg_idx]) maxs[agg_idx] = nv;
            n_num[agg_idx] += 1;
            const cnt_f_gen: f64 = @floatFromInt(n_num[agg_idx]);
            const delta_gen = nv - means[agg_idx];
            means[agg_idx] += delta_gen / cnt_f_gen;
            m2s[agg_idx] += delta_gen * (nv - means[agg_idx]);
        }

        if (opts.rolling) {
            try emitAggSnapshot(writer, fields, total_count, sums, mins, maxs, m2s, n_num, resolveCountAlias(q.fields));
            try writer.flush();
        }
    }

    var out_rec = Record.init(allocator);
    defer out_rec.deinit();
    const count_alias = resolveCountAlias(q.fields);
    if (count_alias.len > 0) {
        try out_rec.fields.put(allocator, try allocator.dupe(u8, count_alias), .{ .number = @floatFromInt(total_count) });
    }
    var agg_idx: usize = 0;
    for (fields) |f| {
        if (f.agg == null) continue;
        defer agg_idx += 1;
        const agg_val: OwnedValue = if (n_num[agg_idx] == 0) .null_val else switch (f.agg.?) {
            .sum => .{ .number = sums[agg_idx] },
            .avg => .{ .number = sums[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .min => .{ .number = mins[agg_idx] },
            .max => .{ .number = maxs[agg_idx] },
            .variance => .{ .number = m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .stddev => .{ .number = @sqrt(m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx]))) },
        };
        try out_rec.fields.put(allocator, try allocator.dupe(u8, if (f.alias) |a| a else f.key), agg_val);
    }
    var one_rec = [1]Record{out_rec};
    if (!opts.rolling) {
        try emitResults(&one_rec, 1, writer, opts.format, opts.raw);
    }
    return 1;
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

    const fields = q.fields orelse return 0;
    var n_aggs: usize = 0;
    for (fields) |f| {
        if (f.agg != null) n_aggs += 1;
    }
    const sums = try allocator.alloc(f64, n_aggs);
    defer allocator.free(sums);
    const mins = try allocator.alloc(f64, n_aggs);
    defer allocator.free(mins);
    const maxs = try allocator.alloc(f64, n_aggs);
    defer allocator.free(maxs);
    const means = try allocator.alloc(f64, n_aggs);
    defer allocator.free(means);
    const m2s = try allocator.alloc(f64, n_aggs);
    defer allocator.free(m2s);
    const n_num = try allocator.alloc(usize, n_aggs);
    defer allocator.free(n_num);
    @memset(sums, 0.0);
    @memset(mins, std.math.floatMax(f64));
    @memset(maxs, -std.math.floatMax(f64));
    @memset(means, 0.0);
    @memset(m2s, 0.0);
    @memset(n_num, 0);
    var total_count: usize = 0;

    while (true) {
        const ev = sc.next() catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        switch (ev) {
            .array_end, .end_of_input => break,
            .object_start => {
                _ = scratch.reset(.retain_capacity);
                var rec = try collectRecord(scratch.allocator(), &sc);
                if (q.where) |w| {
                    if (!recordPassesWhere(scratch.allocator(), &rec, w)) continue;
                }
                total_count += 1;
                var agg_idx: usize = 0;
                for (fields) |f| {
                    if (f.agg == null) continue;
                    defer agg_idx += 1;
                    const val = rec.get(f.key) orelse continue;
                    const nv: f64 = switch (val) {
                        .number => |x| x,
                        .string => |s| std.fmt.parseFloat(f64, s) catch continue,
                        else => continue,
                    };
                    sums[agg_idx] += nv;
                    if (nv < mins[agg_idx]) mins[agg_idx] = nv;
                    if (nv > maxs[agg_idx]) maxs[agg_idx] = nv;
                    n_num[agg_idx] += 1;
                    const cnt_f_sc: f64 = @floatFromInt(n_num[agg_idx]);
                    const delta_sc = nv - means[agg_idx];
                    means[agg_idx] += delta_sc / cnt_f_sc;
                    m2s[agg_idx] += delta_sc * (nv - means[agg_idx]);
                }
            },
            else => {},
        }
    }

    var out_rec = Record.init(allocator);
    defer out_rec.deinit();
    const count_alias = resolveCountAlias(q.fields);
    if (count_alias.len > 0) {
        try out_rec.fields.put(allocator, try allocator.dupe(u8, count_alias), .{ .number = @floatFromInt(total_count) });
    }
    var agg_idx: usize = 0;
    for (fields) |f| {
        if (f.agg == null) continue;
        defer agg_idx += 1;
        const agg_val: OwnedValue = if (n_num[agg_idx] == 0) .null_val else switch (f.agg.?) {
            .sum => .{ .number = sums[agg_idx] },
            .avg => .{ .number = sums[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .min => .{ .number = mins[agg_idx] },
            .max => .{ .number = maxs[agg_idx] },
            .variance => .{ .number = m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])) },
            .stddev => .{ .number = @sqrt(m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx]))) },
        };
        try out_rec.fields.put(allocator, try allocator.dupe(u8, if (f.alias) |a| a else f.key), agg_val);
    }
    var one_rec = [1]Record{out_rec};
    try emitResults(&one_rec, 1, writer, opts.format, opts.raw);
    return 1;
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
    try std.testing.expectApproxEqAbs(@as(f64, 1), matches.items[0].get("id").?.number, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 3), matches.items[2].get("id").?.number, 0.01);
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

    const blob70k = "x" ** 70_000;
    const blob10k = "x" ** 10_000;
    try buf.appendSlice("{\"id\":\"before\",\"score\":1}\n");
    try buf.appendSlice("{\"id\":\"big70k\",\"score\":2,\"blob\":\"");
    try buf.appendSlice(blob70k);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"middle\",\"score\":3}\n");
    try buf.appendSlice("{\"id\":\"big10k\",\"score\":4,\"blob\":\"");
    try buf.appendSlice(blob10k);
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

    const blob = "y" ** 70_000;
    try buf.appendSlice("{\"id\":\"a\",\"dept\":\"eng\",\"blob\":\"");
    try buf.appendSlice(blob);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"b\",\"dept\":\"hr\",\"blob\":\"");
    try buf.appendSlice(blob);
    try buf.appendSlice("\"}\n");
    try buf.appendSlice("{\"id\":\"c\",\"dept\":\"eng\",\"blob\":\"");
    try buf.appendSlice(blob);
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
