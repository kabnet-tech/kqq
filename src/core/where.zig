//! where.zig — WHERE clause evaluation against Records.
//!
//! Evaluates query.WhereClause trees against Record instances.
//! Used by the streaming executors and the expression evaluator (for CASE WHEN).

const std = @import("std");
const query = @import("kq_query");
const regex = @import("kq_regex");
const record_mod = @import("kq_record");
const expr_mod = @import("kq_expr");

const Record = record_mod.Record;
const OwnedValue = record_mod.OwnedValue;
const WhereClause = query.WhereClause;
const Op = query.Op;
const Value = query.Value;

const simpleRegexMatch = regex.simpleRegexMatch;
const globLike = regex.globLike;
const evalExpr = expr_mod.evalExpr;

// ─── OwnedValue comparison ─────────────────────────────────────────────────────

pub fn ownedMatchesCond(ov: OwnedValue, op: Op, qv: Value) bool {
    return switch (op) {
        .eq => switch (qv) {
            .string => |s| ov == .string and std.mem.eql(u8, ov.string, s),
            .number => |n| ov == .number and ov.number == n,
            .boolean => |b| ov == .boolean and ov.boolean == b,
            .null_val => ov == .null_val,
            .string_list => false, // eq against a list makes no sense
        },
        .neq => !ownedMatchesCond(ov, .eq, qv),
        .gt => ov == .number and qv == .number and ov.number > qv.number,
        .lt => ov == .number and qv == .number and ov.number < qv.number,
        .gte => ov == .number and qv == .number and ov.number >= qv.number,
        .lte => ov == .number and qv == .number and ov.number <= qv.number,
        .like => ov == .string and qv == .string and globLike(qv.string, ov.string),
        // ── string operators ─────────────────────────────────────────────────
        .contains => {
            if (ov == .string and qv == .string)
                return std.mem.indexOf(u8, ov.string, qv.string) != null;
            // Array membership: field contains "value" — check if any element matches
            if (ov == .raw and qv == .string and ov.raw.len >= 2 and ov.raw[0] == '[') {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                const parsed = std.json.parseFromSlice(std.json.Value, arena.allocator(), ov.raw, .{}) catch return false;
                if (parsed.value != .array) return false;
                for (parsed.value.array.items) |elem| {
                    switch (elem) {
                        .string => |s| if (std.mem.eql(u8, s, qv.string)) return true,
                        else => {},
                    }
                }
                return false;
            }
            return false;
        },
        .not_contains => !ownedMatchesCond(ov, .contains, qv),
        .starts_with => ov == .string and qv == .string and
            std.mem.startsWith(u8, ov.string, qv.string),
        .ends_with => ov == .string and qv == .string and
            std.mem.endsWith(u8, ov.string, qv.string),
        // ── list operators ───────────────────────────────────────────────────
        .in_list => if (qv == .string_list) blk: {
            // Stringify the field value for comparison
            const field_str: []const u8 = switch (ov) {
                .string => |s| s,
                .number => break :blk false, // number in string list: compare via fmt (todo)
                .boolean => |b| if (b) "true" else "false",
                else => break :blk false,
            };
            for (qv.string_list) |item| {
                if (std.mem.eql(u8, field_str, item)) break :blk true;
            }
            break :blk false;
        } else false,
        .not_in_list => !ownedMatchesCond(ov, .in_list, qv),
        // ── regex operators ──────────────────────────────────────────────────
        .matches => ov == .string and qv == .string and simpleRegexMatch(qv.string, ov.string),
        .not_matches => ov == .string and qv == .string and !simpleRegexMatch(qv.string, ov.string),
        // ── null/existence operators (handled in condOnRecord, not here) ─────
        .is_null, .is_not_null => false, // unreachable via ownedMatchesCond path
        // has_key/not_has_key: key existence handled in condOnRecord before reaching here
        .has_key => true, // if we got a value, key exists
        .not_has_key => false, // if we got a value, key exists → NOT has = false
    };
}

// ─── WHERE tree evaluation ────────────────────────────────────────────────────

pub fn recordPassesWhere(allocator: std.mem.Allocator, rec: *const Record, w: WhereClause) bool {
    return switch (w) {
        .leaf => |c| condOnRecord(allocator, rec, c),
        .and_ => |b| recordPassesWhere(allocator, rec, b.lhs.*) and recordPassesWhere(allocator, rec, b.rhs.*),
        .or_ => |b| recordPassesWhere(allocator, rec, b.lhs.*) or recordPassesWhere(allocator, rec, b.rhs.*),
        .not_ => |inner| !recordPassesWhere(allocator, rec, inner.*),
    };
}

pub fn condOnRecord(allocator: std.mem.Allocator, rec: *const Record, cond: query.Condition) bool {
    // Computed LHS via expression (e.g. `where len(name) > 5`)
    if (cond.lhs_expr) |e| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const ov = evalExpr(arena.allocator(), rec, e) orelse return false;
        // is_null / is_not_null on expr result
        if (cond.op == .is_null) return ov == .null_val;
        if (cond.op == .is_not_null) return ov != .null_val;
        return ownedMatchesCond(ov, cond.op, cond.value);
    }
    // has_key / not_has_key — check key existence regardless of value
    if (cond.op == .has_key) return rec.get(cond.key) != null;
    if (cond.op == .not_has_key) return rec.get(cond.key) == null;
    // is_null / is_not_null must handle the case where the field is absent entirely
    if (cond.op == .is_null) {
        const ov = rec.get(cond.key);
        return ov == null or ov.? == .null_val;
    }
    if (cond.op == .is_not_null) {
        const ov = rec.get(cond.key);
        return ov != null and ov.? != .null_val;
    }
    // in_list / not_in_list: numeric fields compared numerically when list items parse as numbers
    if (cond.op == .in_list or cond.op == .not_in_list) {
        const ov = rec.get(cond.key) orelse return false;
        if (ov == .number and cond.value == .string_list) {
            var found = false;
            for (cond.value.string_list) |item| {
                const n = std.fmt.parseFloat(f64, item) catch continue;
                if (ov.number == n) {
                    found = true;
                    break;
                }
            }
            return if (cond.op == .in_list) found else !found;
        }
        return ownedMatchesCond(ov, cond.op, cond.value);
    }
    const ov = rec.get(cond.key) orelse return false;
    return ownedMatchesCond(ov, cond.op, cond.value);
}