//! raw_where.zig — Raw-byte WHERE fast path.
//!
//! Before parsing a line into a Record, try to evaluate the WHERE clause
//! directly on the raw JSON bytes.  Returns:
//!   true  — record passes WHERE (may still need parse for SELECT projection)
//!   false — record fails WHERE (skip immediately, zero allocations)
//!   null  — can't determine from raw bytes (fall through to full parse)
//!
//! Covers the common case: flat scalar fields, ops eq/neq/gt/lt/gte/lte/is_null/
//! is_not_null/has_key/not_has_key, boolean AND/OR.
//! Falls back to null for: expression LHS, nested fields (dots in key),
//! string comparison with escape sequences, LIKE/IN/regex.

const std = @import("std");
const query = @import("kqq_query");

const Op = query.Op;

/// Scan a raw JSON object for a top-level field by name.
/// Returns a slice of the raw value bytes (number/bool/null/string/"..."),
/// or null if the key is not present at depth 1.
/// Only correct for key names that contain no backslash escapes.
pub fn rawScanField(raw: []const u8, field: []const u8) ?[]const u8 {
    var i: usize = 0;
    var depth: u32 = 0;

    while (i < raw.len) {
        switch (raw[i]) {
            ' ', '\t', '\n', '\r', ',' => i += 1,
            '{', '[' => {
                depth += 1;
                i += 1;
            },
            '}', ']' => {
                if (depth == 0) break;
                depth -= 1;
                i += 1;
            },
            '"' => {
                i += 1; // skip opening quote
                const key_start = i;
                // scan to closing quote, respecting backslash escapes
                while (i < raw.len and raw[i] != '"') {
                    if (raw[i] == '\\') i += 1;
                    i += 1;
                }
                const key_end = i;
                if (i < raw.len) i += 1; // skip closing quote

                // skip optional whitespace then ':'
                while (i < raw.len and (raw[i] == ' ' or raw[i] == '\t')) i += 1;
                if (i < raw.len and raw[i] == ':') i += 1;
                while (i < raw.len and (raw[i] == ' ' or raw[i] == '\t')) i += 1;

                const found_key = raw[key_start..key_end];
                if (depth == 1 and std.mem.eql(u8, found_key, field)) {
                    // This is the field we want. Capture the raw value.
                    const val_start = i;
                    i = rawSkipValue(raw, i);
                    return raw[val_start..i];
                } else {
                    // Not our key — skip the value
                    i = rawSkipValue(raw, i);
                }
            },
            else => i += 1,
        }
    }
    return null;
}

/// Skip one complete JSON value starting at `start`, return index after it.
pub fn rawSkipValue(raw: []const u8, start: usize) usize {
    var i = start;
    if (i >= raw.len) return i;
    // skip leading whitespace
    while (i < raw.len and (raw[i] == ' ' or raw[i] == '\t')) i += 1;
    if (i >= raw.len) return i;
    switch (raw[i]) {
        '"' => {
            i += 1;
            while (i < raw.len and raw[i] != '"') {
                if (raw[i] == '\\') i += 1;
                i += 1;
            }
            if (i < raw.len) i += 1;
        },
        '{', '[' => {
            var d: u32 = 1;
            i += 1;
            while (i < raw.len and d > 0) {
                switch (raw[i]) {
                    '{', '[' => {
                        d += 1;
                        i += 1;
                    },
                    '}', ']' => {
                        d -= 1;
                        i += 1;
                    },
                    '"' => {
                        i += 1;
                        while (i < raw.len and raw[i] != '"') {
                            if (raw[i] == '\\') i += 1;
                            i += 1;
                        }
                        if (i < raw.len) i += 1;
                    },
                    else => i += 1,
                }
            }
        },
        else => {
            // number, bool, null — read until delimiter
            while (i < raw.len) {
                switch (raw[i]) {
                    ',', '}', ']', ' ', '\t', '\n', '\r' => break,
                    else => i += 1,
                }
            }
        },
    }
    return i;
}

/// Compare raw value bytes against a query scalar. Returns null if the
/// comparison can't be decided from raw bytes (e.g. escaped string).
pub fn rawValueCmp(raw_val: []const u8, op: Op, qval: query.Value) ?bool {
    const v = std.mem.trim(u8, raw_val, " \t\n\r");
    switch (qval) {
        .boolean => |b| {
            if (op != .eq and op != .neq) return null;
            const matches = std.mem.eql(u8, v, if (b) "true" else "false");
            return if (op == .eq) matches else !matches;
        },
        .null_val => {
            if (op != .eq and op != .neq) return null;
            const matches = std.mem.eql(u8, v, "null");
            return if (op == .eq) matches else !matches;
        },
        .number => |n| {
            const parsed = std.fmt.parseFloat(f64, v) catch return null;
            return switch (op) {
                .eq => parsed == n,
                .neq => parsed != n,
                .gt => parsed > n,
                .lt => parsed < n,
                .gte => parsed >= n,
                .lte => parsed <= n,
                else => null,
            };
        },
        .string => |s| {
            if (op != .eq and op != .neq) return null;
            // raw_val must be a JSON string literal: "content"
            if (v.len < 2 or v[0] != '"' or v[v.len - 1] != '"') return null;
            const inner = v[1 .. v.len - 1];
            // If inner contains backslash escapes, fall back to full parse
            if (std.mem.indexOfScalar(u8, inner, '\\') != null) return null;
            const matches = std.mem.eql(u8, inner, s);
            return if (op == .eq) matches else !matches;
        },
        .string_list => return null,
    }
}

/// Try to evaluate a WHERE clause directly on raw JSON bytes.
/// Returns null if the clause is too complex for raw evaluation.
pub fn tryWhereOnRaw(raw: []const u8, where: query.WhereClause) ?bool {
    return switch (where) {
        .leaf => |c| tryCondOnRaw(raw, c),
        .and_ => |b| {
            const l = tryWhereOnRaw(raw, b.lhs.*) orelse return null;
            if (!l) return false;
            return tryWhereOnRaw(raw, b.rhs.*);
        },
        .or_ => |b| {
            const l = tryWhereOnRaw(raw, b.lhs.*) orelse return null;
            if (l) return true;
            return tryWhereOnRaw(raw, b.rhs.*);
        },
        .not_ => |inner| {
            const v = tryWhereOnRaw(raw, inner.*) orelse return null;
            return !v;
        },
    };
}

pub fn tryCondOnRaw(raw: []const u8, cond: query.Condition) ?bool {
    // Expression LHS (e.g. len(name) > 5) — needs full parse
    if (cond.lhs_expr != null) return null;
    // Nested field access (dots → PATH_SEP) — needs full parse
    if (std.mem.indexOfScalar(u8, cond.key, '.') != null) return null;

    switch (cond.op) {
        .is_null => {
            const raw_val = rawScanField(raw, cond.key);
            if (raw_val == null) return true; // absent field counts as null
            const v = std.mem.trim(u8, raw_val.?, " \t");
            return std.mem.eql(u8, v, "null");
        },
        .is_not_null => {
            const raw_val = rawScanField(raw, cond.key);
            if (raw_val == null) return false;
            const v = std.mem.trim(u8, raw_val.?, " \t");
            return !std.mem.eql(u8, v, "null");
        },
        .has_key => return rawScanField(raw, cond.key) != null,
        .not_has_key => return rawScanField(raw, cond.key) == null,
        .eq, .neq, .gt, .lt, .gte, .lte => {
            const raw_val = rawScanField(raw, cond.key) orelse return false;
            return rawValueCmp(raw_val, cond.op, cond.value);
        },
        // LIKE / regex / contains / IN — fall back to full parse
        else => return null,
    }
}