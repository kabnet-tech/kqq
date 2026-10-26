//! expr.zig — Expression evaluator and built-in functions.
//!
//! Evaluates query.Expr AST nodes against Record instances, returning OwnedValue.
//! Handles: field access, arithmetic, function calls, CASE WHEN.
//! String results from function calls are heap-allocated (caller owns them).

const std = @import("std");
const query = @import("kqq_query");
const record_mod = @import("kqq_record");
const where_mod = @import("kqq_where");

const Record = record_mod.Record;
const OwnedValue = record_mod.OwnedValue;
const PATH_SEP = record_mod.PATH_SEP;
const formatF64 = record_mod.formatF64;
const Expr = query.Expr;
const FuncName = query.FuncName;
const recordPassesWhere = where_mod.recordPassesWhere;

// ─── Expression evaluator ─────────────────────────────────────────────────────
//
// Evaluates a query.Expr against a Record, returning an OwnedValue.
// String results from function calls are arena-allocated (caller owns them).
// Numeric/boolean results are inline.

/// Evaluate an Expr against a Record. Returned strings are duped into `allocator`.
/// Returns null if the expression cannot be evaluated (missing field, type error, div/0).
pub fn evalExpr(allocator: std.mem.Allocator, rec: *const Record, expr: *const Expr) ?OwnedValue {
    switch (expr.*) {
        .field => |name| {
            const ov = rec.get(name) orelse return null;
            // Return a freshly-owned copy so callers can always take ownership
            return switch (ov) {
                .string => |s| OwnedValue{ .string = allocator.dupe(u8, s) catch return null },
                .number => |n| OwnedValue{ .number = .{
                    .f = n.f,
                    .lit = if (n.lit) |l| allocator.dupe(u8, l) catch return null else null,
                } },
                .boolean => |b| OwnedValue{ .boolean = b },
                .null_val => OwnedValue.null_val,
                .raw => |r| OwnedValue{ .raw = allocator.dupe(u8, r) catch return null },
            };
        },
        .lit_num => |n| return OwnedValue.num(n),
        .lit_str => |s| {
            const dup = allocator.dupe(u8, s) catch return null;
            return OwnedValue{ .string = dup };
        },
        .add => |b| {
            const l = evalExpr(allocator, rec, b.lhs) orelse return null;
            const r = evalExpr(allocator, rec, b.rhs) orelse return null;
            return numericBinOp(l, r, .add);
        },
        .sub => |b| {
            const l = evalExpr(allocator, rec, b.lhs) orelse return null;
            const r = evalExpr(allocator, rec, b.rhs) orelse return null;
            return numericBinOp(l, r, .sub);
        },
        .mul => |b| {
            const l = evalExpr(allocator, rec, b.lhs) orelse return null;
            const r = evalExpr(allocator, rec, b.rhs) orelse return null;
            return numericBinOp(l, r, .mul);
        },
        .div => |b| {
            const l = evalExpr(allocator, rec, b.lhs) orelse return null;
            const r = evalExpr(allocator, rec, b.rhs) orelse return null;
            return numericBinOp(l, r, .div);
        },
        .mod => |b| {
            const l = evalExpr(allocator, rec, b.lhs) orelse return null;
            const r = evalExpr(allocator, rec, b.rhs) orelse return null;
            return numericBinOp(l, r, .mod);
        },
        .func => |f| return evalFunc(allocator, rec, f.name, f.args),
        .case_when => |*c| {
            for (c.arms) |arm| {
                if (recordPassesWhere(allocator, rec, arm.cond)) {
                    return evalExpr(allocator, rec, arm.result);
                }
            }
            if (c.else_expr) |e| return evalExpr(allocator, rec, e);
            return OwnedValue.null_val;
        },
    }
}

const BinOp = enum { add, sub, mul, div, mod };

pub fn toF64(ov: OwnedValue) ?f64 {
    return switch (ov) {
        .number => |n| n.f,
        .string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
}

/// Arithmetic on two values. Returns null (cannot evaluate) when either side
/// is not numeric; division or modulo by zero yields SQL NULL (`.null_val`),
/// so a computed column is emitted as `null` rather than silently dropped.
pub fn numericBinOp(l: OwnedValue, r: OwnedValue, op: BinOp) ?OwnedValue {
    const a = toF64(l) orelse return null;
    const b = toF64(r) orelse return null;
    if ((op == .div or op == .mod) and b == 0.0) return OwnedValue.null_val;
    const result: f64 = switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => a / b,
        .mod => @rem(a, b),
    };
    return OwnedValue.num(result);
}

/// Append a JSON-escaped string literal (with surrounding quotes) to a byte list.
pub fn appendJsonStr(buf: *std.array_list.Managed(u8), s: []const u8) !void {
    try buf.append('"');
    for (s) |c| {
        if (c == '"') try buf.appendSlice("\\\"") else if (c == '\\') try buf.appendSlice("\\\\") else if (c < 0x20) {
            // Control characters: emit \uXXXX
            var tmp: [6]u8 = undefined;
            const enc = std.fmt.bufPrint(&tmp, "\\u{X:0>4}", .{c}) catch continue;
            try buf.appendSlice(enc);
        } else try buf.append(c);
    }
    try buf.append('"');
}

/// Append an OwnedValue serialized as JSON to a byte list.
pub fn appendOwnedJson(buf: *std.array_list.Managed(u8), ov: OwnedValue) !void {
    switch (ov) {
        .null_val => try buf.appendSlice("null"),
        .boolean => |b| try buf.appendSlice(if (b) "true" else "false"),
        .number => |n| {
            var tmp: [64]u8 = undefined;
            try buf.appendSlice(n.text(&tmp));
        },
        .string => |s| try appendJsonStr(buf, s),
        .raw => |r| try buf.appendSlice(r),
    }
}

/// Append a std.json.Value serialized as JSON to a byte list.
pub fn appendParsedJson(buf: *std.array_list.Managed(u8), val: std.json.Value) !void {
    switch (val) {
        .null => try buf.appendSlice("null"),
        .bool => |b| try buf.appendSlice(if (b) "true" else "false"),
        .integer => |n| {
            var tmp: [32]u8 = undefined;
            const s = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch return error.OutOfMemory;
            try buf.appendSlice(s);
        },
        .float => |f| {
            var tmp: [64]u8 = undefined;
            try buf.appendSlice(formatF64(&tmp, f));
        },
        .number_string => |s| try buf.appendSlice(s),
        .string => |s| try appendJsonStr(buf, s),
        .array => |arr| {
            try buf.append('[');
            for (arr.items, 0..) |item, j| {
                if (j > 0) try buf.append(',');
                try appendParsedJson(buf, item);
            }
            try buf.append(']');
        },
        .object => |obj| {
            try buf.append('{');
            var it = obj.iterator();
            var first2 = true;
            while (it.next()) |entry| {
                if (!first2) try buf.append(',');
                first2 = false;
                try appendJsonStr(buf, entry.key_ptr.*);
                try buf.append(':');
                try appendParsedJson(buf, entry.value_ptr.*);
            }
            try buf.append('}');
        },
    }
}

// ─── Date/time helpers ───────────────────────────────────────────────────────

const native_os = @import("builtin").os.tag;

/// UTC wall-clock seconds since the Unix epoch. Returns 0 on unsupported targets.
fn utcNowSecs() i64 {
    return @intCast(@divTrunc(utcNowNanos(), std.time.ns_per_s));
}

/// UTC wall-clock milliseconds since the Unix epoch.
fn utcNowMs() i64 {
    return @intCast(@divTrunc(utcNowNanos(), std.time.ns_per_ms));
}

/// UTC wall-clock nanoseconds since the Unix epoch (1970-01-01T00:00:00Z).
/// Returns 0 on unsupported targets.
///
/// Platform strategy:
///   Linux   — std.os.linux.clock_gettime (direct syscall, no libc)
///   Windows — RtlGetSystemTimePrecise (ntdll, no libc)
///   macOS   — std.c.clock_gettime (libc, always available on macOS)
///   Other   — std.c.clock_gettime (libc)
fn utcNowNanos() i128 {
    if (native_os == .windows) {
        // RtlGetSystemTimePrecise returns 100-nanosecond intervals since
        // 1601-01-01 (the Windows epoch). Adjust to Unix epoch.
        const hns: i64 = std.os.windows.ntdll.RtlGetSystemTimePrecise();
        const unix_hns: i64 = hns + std.time.epoch.windows * (std.time.ns_per_s / 100);
        return @as(i128, unix_hns) * 100;
    }

    if (native_os == .linux) {
        // Direct syscall — no libc dependency.
        var ts: std.os.linux.timespec = undefined;
        const rc = std.os.linux.clock_gettime(std.os.linux.CLOCK.REALTIME, &ts);
        if (rc != 0) return 0;
        const secs: i128 = @intCast(ts.sec);
        const nsec: i128 = @intCast(ts.nsec);
        return secs * std.time.ns_per_s + nsec;
    }

    // macOS and other POSIX systems: use libc clock_gettime.
    var ts: std.c.timespec = undefined;
    const rc = std.c.clock_gettime(std.c.CLOCK.REALTIME, &ts);
    if (rc != 0) return 0;
    const secs: i128 = @intCast(ts.sec);
    const nsec: i128 = @intCast(ts.nsec);
    return secs * std.time.ns_per_s + nsec;
}

/// Howard Hinnant's O(1) algorithm: civil date → days since 1970-01-01.
fn daysFromCivil(year: i32, month: i32, day: i32) i64 {
    const y: i32 = if (month <= 2) year - 1 else year;
    const era: i32 = @divFloor(y, 400);
    const yoe: i32 = y - era * 400;
    const doy: i32 = @divTrunc(153 * (if (month > 2) month - 3 else month + 9) + 2, 5) + day - 1;
    const doe: i32 = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return @as(i64, era) * 146097 + @as(i64, doe) - 719468;
}

/// Parse an ISO 8601 string to UTC epoch seconds.
/// Handles: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SSZ, YYYY-MM-DDTHH:MM:SS.mmmZ
/// Returns null for unrecognised formats.
fn parseIsoSecs(s: []const u8) ?i64 {
    if (s.len < 10) return null;
    const year = std.fmt.parseInt(i32, s[0..4], 10) catch return null;
    if (s[4] != '-') return null;
    const month = std.fmt.parseInt(i32, s[5..7], 10) catch return null;
    if (s[7] != '-') return null;
    const day = std.fmt.parseInt(i32, s[8..10], 10) catch return null;
    if (month < 1 or month > 12 or day < 1 or day > 31) return null;
    const base: i64 = daysFromCivil(year, month, day) * 86400;
    if (s.len >= 19 and (s[10] == 'T' or s[10] == ' ')) {
        const hour = std.fmt.parseInt(i64, s[11..13], 10) catch return null;
        if (s[13] != ':') return null;
        const min = std.fmt.parseInt(i64, s[14..16], 10) catch return null;
        if (s[16] != ':') return null;
        const sec = std.fmt.parseInt(i64, s[17..19], 10) catch return null;
        return base + hour * 3600 + min * 60 + sec;
    }
    return base;
}

/// f64 → i64, or null when the value is NaN, infinite or out of range
/// (a bare @intFromFloat on such values is undefined behaviour).
fn toI64(f: f64) ?i64 {
    if (!std.math.isFinite(f) or @abs(f) >= 9.2e18) return null;
    return @intFromFloat(f);
}

/// Clamp a numeric argument into a usable length/index (NaN → 0). The cap
/// fits usize on 32-bit targets too.
fn toLen(f: f64) usize {
    if (!(f > 0)) return 0;
    const cap = std.math.maxInt(u32);
    if (f >= cap) return cap;
    return @intFromFloat(f);
}

/// Coerce an OwnedValue to epoch seconds. Numbers pass through; strings are parsed.
fn toEpochSecs(v: OwnedValue) ?i64 {
    return switch (v) {
        .number => |n| toI64(n.f),
        .string => |s| parseIsoSecs(s),
        else => null,
    };
}

/// 9999-12-31T23:59:59Z. Calendar decomposition walks year by year, so larger
/// inputs are rejected rather than looping for minutes.
const max_epoch_secs: i64 = 253402300799;

/// Format epoch seconds (>= 0) as "YYYY-MM-DDTHH:MM:SSZ" into allocator.
/// Returns null for pre-epoch times (negative seconds) to avoid u64 overflow.
fn epochToIso(allocator: std.mem.Allocator, secs: i64) ?[]u8 {
    if (secs < 0 or secs > max_epoch_secs) return null;
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(secs) };
    const ed = es.getEpochDay();
    const yd = ed.calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    var buf: [24]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        yd.year,
        @as(u8, @intFromEnum(md.month)),
        md.day_index + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
    }) catch return null;
    return allocator.dupe(u8, s) catch null;
}

pub fn evalFunc(allocator: std.mem.Allocator, rec: *const Record, name: FuncName, args: []*Expr) ?OwnedValue {
    // Use a short-lived arena for all intermediate arg evaluations.
    // Only the final result (a single OwnedValue) is duped into `allocator`.
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const tmp = arena.allocator();

    switch (name) {
        .upper => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            if (v != .string) return null;
            const s = allocator.dupe(u8, v.string) catch return null;
            for (s) |*c| c.* = std.ascii.toUpper(c.*);
            return OwnedValue{ .string = s };
        },
        .lower => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            if (v != .string) return null;
            const s = allocator.dupe(u8, v.string) catch return null;
            for (s) |*c| c.* = std.ascii.toLower(c.*);
            return OwnedValue{ .string = s };
        },
        .len => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const l: f64 = switch (v) {
                .string => |s| @floatFromInt(s.len),
                .raw => |r| blk: {
                    // Array stored as raw JSON "[...]" — count elements
                    if (r.len >= 2 and r[0] == '[') {
                        var parsed = std.json.parseFromSlice(std.json.Value, tmp, r, .{}) catch break :blk 0.0;
                        defer parsed.deinit();
                        if (parsed.value == .array) break :blk @floatFromInt(parsed.value.array.items.len);
                    }
                    break :blk 0.0;
                },
                else => return null,
            };
            return OwnedValue.num(l);
        },
        .trim => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            if (v != .string) return null;
            const trimmed = std.mem.trim(u8, v.string, " \t\n\r");
            return OwnedValue{ .string = allocator.dupe(u8, trimmed) catch return null };
        },
        .round => {
            if (args.len < 1 or args.len > 2) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            const decimals: f64 = if (args.len == 2) blk: {
                const d = evalExpr(tmp, rec, args[1]) orelse break :blk 0.0;
                break :blk toF64(d) orelse 0.0;
            } else 0.0;
            const factor = std.math.pow(f64, 10.0, decimals);
            return OwnedValue.num(@round(n * factor) / factor);
        },
        .floor => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(@floor(n));
        },
        .ceil => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(@ceil(n));
        },
        .abs => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(@abs(n));
        },
        .concat => {
            // concat(a, b, ...) — join all string/number args into one string
            var buf = std.array_list.Managed(u8).init(tmp);
            // no defer deinit: arena handles it
            for (args) |arg| {
                const v = evalExpr(tmp, rec, arg) orelse return null;
                switch (v) {
                    .string => |s| buf.appendSlice(s) catch return null,
                    .number => |nx| {
                        var ftmp: [64]u8 = undefined;
                        buf.appendSlice(nx.text(&ftmp)) catch return null;
                    },
                    else => return null,
                }
            }
            return OwnedValue{ .string = allocator.dupe(u8, buf.items) catch return null };
        },
        .substr => {
            // substr(str, start[, length])  — 0-based start
            if (args.len < 2) return null;
            const sv = evalExpr(tmp, rec, args[0]) orelse return null;
            if (sv != .string) return null;
            const start_v = evalExpr(tmp, rec, args[1]) orelse return null;
            const start = toLen(toF64(start_v) orelse return null);
            const slen: usize = if (args.len >= 3) blk: {
                const lv = evalExpr(tmp, rec, args[2]) orelse break :blk sv.string.len;
                break :blk if (toF64(lv)) |l| toLen(l) else sv.string.len;
            } else sv.string.len;
            if (start >= sv.string.len) {
                return OwnedValue{ .string = allocator.dupe(u8, "") catch return null };
            }
            const end = start + @min(slen, sv.string.len - start);
            return OwnedValue{ .string = allocator.dupe(u8, sv.string[start..end]) catch return null };
        },
        .to_str => {
            if (args.len != 1) return null;
            const v = evalExpr(allocator, rec, args[0]) orelse return null;
            switch (v) {
                .string => |s| return OwnedValue{ .string = allocator.dupe(u8, s) catch return null },
                .number => |n| {
                    var buf: [64]u8 = undefined;
                    return OwnedValue{ .string = allocator.dupe(u8, n.text(&buf)) catch return null };
                },
                .boolean => |b| return OwnedValue{ .string = allocator.dupe(u8, if (b) "true" else "false") catch return null },
                else => return null,
            }
        },
        .to_number => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            switch (v) {
                .number => |n| return OwnedValue.num(n.f),
                .string => |s| {
                    const n = std.fmt.parseFloat(f64, s) catch return null;
                    return OwnedValue.num(n);
                },
                .boolean => |b| return OwnedValue.num(if (b) 1.0 else 0.0),
                else => return null,
            }
        },
        .format => {
            // format("Hello {name}, age {age}") — interpolate {field} tokens from record
            // OR format("template", field1, field2, ...) — positional {0}, {1} (future)
            if (args.len < 1) return null;
            const tmpl_v = evalExpr(tmp, rec, args[0]) orelse return null;
            if (tmpl_v != .string) return null;
            const tmpl = tmpl_v.string;
            var out = std.array_list.Managed(u8).init(tmp);
            var i: usize = 0;
            while (i < tmpl.len) {
                if (tmpl[i] == '{') {
                    const end = std.mem.indexOfScalarPos(u8, tmpl, i + 1, '}') orelse {
                        out.append(tmpl[i]) catch return null;
                        i += 1;
                        continue;
                    };
                    const fname = tmpl[i + 1 .. end];
                    if (rec.get(fname)) |fv| {
                        switch (fv) {
                            .string => |s| out.appendSlice(s) catch return null,
                            .number => |n| {
                                var nbuf: [64]u8 = undefined;
                                out.appendSlice(n.text(&nbuf)) catch return null;
                            },
                            .boolean => |b| out.appendSlice(if (b) "true" else "false") catch return null,
                            else => out.appendSlice("") catch return null,
                        }
                    } else {
                        // field not found — emit empty string
                    }
                    i = end + 1;
                } else {
                    out.append(tmpl[i]) catch return null;
                    i += 1;
                }
            }
            return OwnedValue{ .string = allocator.dupe(u8, out.items) catch return null };
        },
        .ifhas => {
            // ifhas(field, then_val[, else_val]) — if field exists and is not null
            if (args.len < 2) return null;
            const field_expr = args[0];
            if (field_expr.* != .field) return null;
            const key = field_expr.field;
            const exists = rec.get(key) != null;
            if (exists) {
                return evalExpr(allocator, rec, args[1]);
            } else if (args.len >= 3) {
                return evalExpr(allocator, rec, args[2]);
            } else {
                return OwnedValue.null_val;
            }
        },
        .coalesce => {
            // coalesce(a, b, c, ...) — return first non-null value
            for (args) |arg| {
                const v = evalExpr(allocator, rec, arg) orelse continue;
                if (v != .null_val) return v;
            }
            return OwnedValue.null_val;
        },
        .replace => {
            // replace(str, from, to)
            if (args.len != 3) return null;
            const sv = evalExpr(tmp, rec, args[0]) orelse return null;
            if (sv != .string) return null;
            const from_v = evalExpr(tmp, rec, args[1]) orelse return null;
            if (from_v != .string) return null;
            const to_v = evalExpr(tmp, rec, args[2]) orelse return null;
            const to_str2 = switch (to_v) {
                .string => |s| s,
                else => return null,
            };
            // Replace all occurrences
            var out2 = std.array_list.Managed(u8).init(tmp);
            var src = sv.string;
            while (std.mem.indexOf(u8, src, from_v.string)) |idx| {
                out2.appendSlice(src[0..idx]) catch return null;
                out2.appendSlice(to_str2) catch return null;
                src = src[idx + from_v.string.len ..];
            }
            out2.appendSlice(src) catch return null;
            return OwnedValue{ .string = allocator.dupe(u8, out2.items) catch return null };
        },
        .lpad => {
            // lpad(str, len, pad_char)
            if (args.len != 3) return null;
            const sv = evalExpr(tmp, rec, args[0]) orelse return null;
            if (sv != .string) return null;
            const len_v = evalExpr(tmp, rec, args[1]) orelse return null;
            const target_len = toLen(toF64(len_v) orelse return null);
            const pad_v = evalExpr(tmp, rec, args[2]) orelse return null;
            if (pad_v != .string or pad_v.string.len == 0) return null;
            const pad_ch = pad_v.string[0];
            if (sv.string.len >= target_len) return OwnedValue{ .string = allocator.dupe(u8, sv.string) catch return null };
            const pad_count = target_len - sv.string.len;
            const out3 = allocator.alloc(u8, target_len) catch return null;
            @memset(out3[0..pad_count], pad_ch);
            @memcpy(out3[pad_count..], sv.string);
            return OwnedValue{ .string = out3 };
        },
        .rpad => {
            // rpad(str, len, pad_char)
            if (args.len != 3) return null;
            const sv = evalExpr(tmp, rec, args[0]) orelse return null;
            if (sv != .string) return null;
            const len_v = evalExpr(tmp, rec, args[1]) orelse return null;
            const target_len = toLen(toF64(len_v) orelse return null);
            const pad_v = evalExpr(tmp, rec, args[2]) orelse return null;
            if (pad_v != .string or pad_v.string.len == 0) return null;
            const pad_ch = pad_v.string[0];
            if (sv.string.len >= target_len) return OwnedValue{ .string = allocator.dupe(u8, sv.string) catch return null };
            const out4 = allocator.alloc(u8, target_len) catch return null;
            @memcpy(out4[0..sv.string.len], sv.string);
            @memset(out4[sv.string.len..], pad_ch);
            return OwnedValue{ .string = out4 };
        },
        .split => {
            // split(str, delim) → raw JSON array e.g. ["a","b","c"]
            if (args.len != 2) return null;
            const sv = evalExpr(tmp, rec, args[0]) orelse return null;
            if (sv != .string) return null;
            const dv = evalExpr(tmp, rec, args[1]) orelse return null;
            if (dv != .string or dv.string.len == 0) return null;
            var out5 = std.array_list.Managed(u8).init(tmp);
            out5.append('[') catch return null;
            var src5 = sv.string;
            var first5 = true;
            while (true) {
                if (!first5) out5.append(',') catch return null;
                first5 = false;
                if (std.mem.indexOf(u8, src5, dv.string)) |idx| {
                    out5.append('"') catch return null;
                    // JSON-escape the token
                    for (src5[0..idx]) |c| {
                        if (c == '"') out5.appendSlice("\\\"") catch return null else if (c == '\\') out5.appendSlice("\\\\") catch return null else out5.append(c) catch return null;
                    }
                    out5.append('"') catch return null;
                    src5 = src5[idx + dv.string.len ..];
                } else {
                    out5.append('"') catch return null;
                    for (src5) |c| {
                        if (c == '"') out5.appendSlice("\\\"") catch return null else if (c == '\\') out5.appendSlice("\\\\") catch return null else out5.append(c) catch return null;
                    }
                    out5.append('"') catch return null;
                    break;
                }
            }
            out5.append(']') catch return null;
            return OwnedValue{ .raw = allocator.dupe(u8, out5.items) catch return null };
        },
        .type_of => {
            // type(field) → "null" | "boolean" | "number" | "string" | "array" | "object"
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return OwnedValue{ .string = allocator.dupe(u8, "null") catch return null };
            const type_name: []const u8 = switch (v) {
                .null_val => "null",
                .boolean => "boolean",
                .number => "number",
                .string => "string",
                .raw => |r| if (r.len > 0 and r[0] == '[') "array" else "object",
            };
            return OwnedValue{ .string = allocator.dupe(u8, type_name) catch return null };
        },
        .keys => {
            // keys()        → JSON array of all top-level field names of the record
            // keys(field)   → JSON array of keys of a nested object field
            if (args.len == 0) {
                var arr = std.array_list.Managed(u8).init(tmp);
                arr.append('[') catch return null;
                var first = true;
                var it = rec.fields.iterator();
                while (it.next()) |entry| {
                    if (std.mem.indexOfScalar(u8, entry.key_ptr.*, PATH_SEP) != null) continue;
                    if (!first) arr.append(',') catch return null;
                    first = false;
                    appendJsonStr(&arr, entry.key_ptr.*) catch return null;
                }
                arr.append(']') catch return null;
                return OwnedValue{ .raw = allocator.dupe(u8, arr.items) catch return null };
            } else if (args.len == 1) {
                const v = evalExpr(tmp, rec, args[0]) orelse return null;
                if (v != .raw) return null;
                var parsed = std.json.parseFromSlice(std.json.Value, tmp, v.raw, .{}) catch return null;
                defer parsed.deinit();
                if (parsed.value != .object) return null;
                var arr = std.array_list.Managed(u8).init(tmp);
                arr.append('[') catch return null;
                var first2 = true;
                for (parsed.value.object.keys()) |k| {
                    if (!first2) arr.append(',') catch return null;
                    first2 = false;
                    appendJsonStr(&arr, k) catch return null;
                }
                arr.append(']') catch return null;
                return OwnedValue{ .raw = allocator.dupe(u8, arr.items) catch return null };
            }
            return null;
        },
        .values => {
            // values()       → JSON array of all top-level field values of the record
            // values(field)  → JSON array of values of a nested object field
            if (args.len == 0) {
                var arr = std.array_list.Managed(u8).init(tmp);
                arr.append('[') catch return null;
                var first = true;
                var it = rec.fields.iterator();
                while (it.next()) |entry| {
                    if (std.mem.indexOfScalar(u8, entry.key_ptr.*, PATH_SEP) != null) continue;
                    if (!first) arr.append(',') catch return null;
                    first = false;
                    appendOwnedJson(&arr, entry.value_ptr.*) catch return null;
                }
                arr.append(']') catch return null;
                return OwnedValue{ .raw = allocator.dupe(u8, arr.items) catch return null };
            } else if (args.len == 1) {
                const v = evalExpr(tmp, rec, args[0]) orelse return null;
                if (v != .raw) return null;
                var parsed = std.json.parseFromSlice(std.json.Value, tmp, v.raw, .{}) catch return null;
                defer parsed.deinit();
                if (parsed.value != .object) return null;
                var arr = std.array_list.Managed(u8).init(tmp);
                arr.append('[') catch return null;
                var vit = parsed.value.object.iterator();
                var first2 = true;
                while (vit.next()) |entry| {
                    if (!first2) arr.append(',') catch return null;
                    first2 = false;
                    appendParsedJson(&arr, entry.value_ptr.*) catch return null;
                }
                arr.append(']') catch return null;
                return OwnedValue{ .raw = allocator.dupe(u8, arr.items) catch return null };
            }
            return null;
        },
        .to_entries => {
            // to_entries()      → [{key:"k",value:v},...] for the whole record
            // to_entries(field) → same for a nested object field
            if (args.len == 0) {
                var arr = std.array_list.Managed(u8).init(tmp);
                arr.append('[') catch return null;
                var first = true;
                var it = rec.fields.iterator();
                while (it.next()) |entry| {
                    if (std.mem.indexOfScalar(u8, entry.key_ptr.*, PATH_SEP) != null) continue;
                    if (!first) arr.append(',') catch return null;
                    first = false;
                    arr.appendSlice("{\"key\":") catch return null;
                    appendJsonStr(&arr, entry.key_ptr.*) catch return null;
                    arr.appendSlice(",\"value\":") catch return null;
                    appendOwnedJson(&arr, entry.value_ptr.*) catch return null;
                    arr.append('}') catch return null;
                }
                arr.append(']') catch return null;
                return OwnedValue{ .raw = allocator.dupe(u8, arr.items) catch return null };
            } else if (args.len == 1) {
                const v = evalExpr(tmp, rec, args[0]) orelse return null;
                if (v != .raw) return null;
                var parsed = std.json.parseFromSlice(std.json.Value, tmp, v.raw, .{}) catch return null;
                defer parsed.deinit();
                if (parsed.value != .object) return null;
                var arr = std.array_list.Managed(u8).init(tmp);
                arr.append('[') catch return null;
                var eit = parsed.value.object.iterator();
                var first2 = true;
                while (eit.next()) |entry| {
                    if (!first2) arr.append(',') catch return null;
                    first2 = false;
                    arr.appendSlice("{\"key\":") catch return null;
                    appendJsonStr(&arr, entry.key_ptr.*) catch return null;
                    arr.appendSlice(",\"value\":") catch return null;
                    appendParsedJson(&arr, entry.value_ptr.*) catch return null;
                    arr.append('}') catch return null;
                }
                arr.append(']') catch return null;
                return OwnedValue{ .raw = allocator.dupe(u8, arr.items) catch return null };
            }
            return null;
        },
        .now => {
            if (args.len != 0) return null;
            return OwnedValue{ .string = epochToIso(allocator, utcNowSecs()) orelse return null };
        },
        .now_epoch => {
            if (args.len != 0) return null;
            return OwnedValue.num(@floatFromInt(utcNowSecs()));
        },
        .now_ms => {
            if (args.len != 0) return null;
            return OwnedValue.num(@floatFromInt(utcNowMs()));
        },
        .from_epoch => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const secs = toEpochSecs(v) orelse return null;
            return OwnedValue{ .string = epochToIso(allocator, secs) orelse return null };
        },
        .from_epoch_ms => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const ms: i64 = switch (v) {
                .number => |n| toI64(n.f) orelse return null,
                .string => |s| blk: {
                    const secs = parseIsoSecs(s) orelse return null;
                    break :blk secs * 1000;
                },
                else => return null,
            };
            return OwnedValue{ .string = epochToIso(allocator, @divTrunc(ms, 1000)) orelse return null };
        },
        .to_epoch => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const secs = toEpochSecs(v) orelse return null;
            return OwnedValue.num(@floatFromInt(secs));
        },
        .to_epoch_ms => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const secs = toEpochSecs(v) orelse return null;
            return OwnedValue.num(@as(f64, @floatFromInt(secs)) * 1000);
        },
        .date_part => {
            if (args.len != 2) return null;
            const ts_v = evalExpr(tmp, rec, args[0]) orelse return null;
            const comp_v = evalExpr(tmp, rec, args[1]) orelse return null;
            if (comp_v != .string) return null;
            const comp = comp_v.string;
            const secs = toEpochSecs(ts_v) orelse return null;
            if (std.mem.eql(u8, comp, "epoch")) return OwnedValue.num(@floatFromInt(secs));
            if (secs < 0 or secs > max_epoch_secs) return null; // pre-1970 / post-9999 not supported
            const es = std.time.epoch.EpochSeconds{ .secs = @intCast(secs) };
            if (std.mem.eql(u8, comp, "hour")) {
                return OwnedValue.num(@floatFromInt(es.getDaySeconds().getHoursIntoDay()));
            } else if (std.mem.eql(u8, comp, "minute")) {
                return OwnedValue.num(@floatFromInt(es.getDaySeconds().getMinutesIntoHour()));
            } else if (std.mem.eql(u8, comp, "second")) {
                return OwnedValue.num(@floatFromInt(es.getDaySeconds().getSecondsIntoMinute()));
            }
            const ed = es.getEpochDay();
            const yd = ed.calculateYearDay();
            if (std.mem.eql(u8, comp, "year")) {
                return OwnedValue.num(@floatFromInt(yd.year));
            }
            const md = yd.calculateMonthDay();
            if (std.mem.eql(u8, comp, "month")) {
                return OwnedValue.num(@floatFromInt(@intFromEnum(md.month)));
            } else if (std.mem.eql(u8, comp, "day")) {
                return OwnedValue.num(@floatFromInt(md.day_index + 1));
            }
            return null;
        },
        .epoch_min => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(n * 60.0);
        },
        .epoch_hour => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(n * 3600.0);
        },
        .epoch_day => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(n * 86400.0);
        },
        .epoch_week => {
            if (args.len != 1) return null;
            const v = evalExpr(tmp, rec, args[0]) orelse return null;
            const n = toF64(v) orelse return null;
            return OwnedValue.num(n * 604800.0);
        },
    }
}
