//! ast.zig — Query AST type definitions.
//!
//! All type definitions for the query language: Expr, FuncName, SelectField,
//! Op, Value, Condition, WhereClause, Query, etc.
//! These types are imported by every other module in the system.

const std = @import("std");

pub const ExprTag = enum {
    field, // bare field reference: `name`
    lit_num, // numeric literal: 42.0
    lit_str, // string literal: "hello"
    add,
    sub,
    mul,
    div,
    mod, // binary arithmetic
    func, // function call: upper(name)
    case_when, // CASE WHEN cond THEN expr ... ELSE expr END
};

pub const FuncName = enum {
    upper,
    lower,
    len,
    round,
    floor,
    ceil,
    trim,
    concat,
    substr,
    abs,
    to_str, // number/bool → string
    to_number, // string → number
    format, // format("template {field}", ...) → string (f-string style)
    ifhas, // ifhas(field, then_expr, else_expr)
    coalesce, // coalesce(a, b, c) → first non-null
    replace, // replace(str, from, to)
    lpad, // lpad(str, len, char)
    rpad, // rpad(str, len, char)
    split, // split(str, delim) → raw JSON array string
    type_of, // type(field) → "string"|"number"|"boolean"|"null"|"array"|"object"
    keys, // keys()        → JSON array of all top-level field names
    // keys(field)   → JSON array of keys of a nested object field
    values, // values()       → JSON array of all top-level field values
    // values(field)  → JSON array of values of a nested object field
    to_entries, // to_entries()      → [{"key":k,"value":v},...] for whole record
    // to_entries(field) → same for a nested object field
    // Date/time functions (UTC only)
    now, // now() → current UTC ISO string e.g. "2026-07-08T05:13:48Z"
    now_epoch, // now_epoch() → current UTC Unix epoch seconds (number)
    now_ms, // now_ms() → current UTC epoch milliseconds (number)
    from_epoch, // from_epoch(n) → ISO string from epoch seconds
    from_epoch_ms, // from_epoch_ms(n) → ISO string from epoch milliseconds
    to_epoch, // to_epoch(str|n) → epoch seconds; ISO strings parsed, numbers pass through
    to_epoch_ms, // to_epoch_ms(str|n) → epoch milliseconds
    date_part, // date_part(ts, component) → number; component: year/month/day/hour/minute/second/epoch
    // Epoch duration helpers — enable pure-epoch date arithmetic with no calendar complexity
    epoch_min, // epoch_min(n) → n * 60   (seconds in n minutes)
    epoch_hour, // epoch_hour(n) → n * 3600
    epoch_day, // epoch_day(n) → n * 86400
    epoch_week, // epoch_week(n) → n * 604800
};

pub const Expr = union(ExprTag) {
    field: []const u8, // heap-owned field path
    lit_num: f64,
    lit_str: []const u8, // heap-owned
    add: BinExpr,
    sub: BinExpr,
    mul: BinExpr,
    div: BinExpr,
    mod: BinExpr,
    func: FuncExpr,
    case_when: CaseExpr,

    pub fn deinit(self: *Expr, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .field => |s| allocator.free(s),
            .lit_str => |s| allocator.free(s),
            .add, .sub, .mul, .div, .mod => |*b| {
                b.lhs.deinit(allocator);
                b.rhs.deinit(allocator);
                allocator.destroy(b.lhs);
                allocator.destroy(b.rhs);
            },
            .func => |*f| {
                for (f.args) |a| {
                    a.deinit(allocator);
                    allocator.destroy(a);
                }
                allocator.free(f.args);
            },
            .case_when => |*c| {
                for (c.arms) |*arm| {
                    arm.cond.deinit(allocator);
                    arm.result.deinit(allocator);
                    allocator.destroy(arm.result);
                }
                allocator.free(c.arms);
                if (c.else_expr) |e| {
                    e.deinit(allocator);
                    allocator.destroy(e);
                }
            },
            .lit_num => {},
        }
    }
};

/// Deep-clone a heap-allocated Expr into `allocator`. Used by GROUP BY alias resolution
/// so the GROUP BY key can own its expression independently of the SELECT field list.
pub fn cloneBinExpr(allocator: std.mem.Allocator, b: BinExpr) error{OutOfMemory}!BinExpr {
    const lhs = try cloneExpr(allocator, b.lhs);
    errdefer {
        lhs.deinit(allocator);
        allocator.destroy(lhs);
    }
    const rhs = try cloneExpr(allocator, b.rhs);
    return .{ .lhs = lhs, .rhs = rhs };
}

pub fn cloneExpr(allocator: std.mem.Allocator, src: *const Expr) error{OutOfMemory}!*Expr {
    const dst = try allocator.create(Expr);
    errdefer allocator.destroy(dst);
    dst.* = switch (src.*) {
        .field => |s| .{ .field = try allocator.dupe(u8, s) },
        .lit_num => |n| .{ .lit_num = n },
        .lit_str => |s| .{ .lit_str = try allocator.dupe(u8, s) },
        .add => |b| .{ .add = try cloneBinExpr(allocator, b) },
        .sub => |b| .{ .sub = try cloneBinExpr(allocator, b) },
        .mul => |b| .{ .mul = try cloneBinExpr(allocator, b) },
        .div => |b| .{ .div = try cloneBinExpr(allocator, b) },
        .mod => |b| .{ .mod = try cloneBinExpr(allocator, b) },
        .func => |f| blk: {
            const args = try allocator.alloc(*Expr, f.args.len);
            errdefer allocator.free(args);
            var n: usize = 0;
            while (n < f.args.len) : (n += 1) {
                args[n] = try cloneExpr(allocator, f.args[n]);
            }
            break :blk .{ .func = .{ .name = f.name, .args = args } };
        },
        // case_when not supported in GROUP BY context — treat as OOM to fail gracefully
        .case_when => return error.OutOfMemory,
    };
    return dst;
}

pub const BinExpr = struct {
    lhs: *Expr,
    rhs: *Expr,
};

pub const FuncExpr = struct {
    name: FuncName,
    args: []*Expr, // heap-owned slice of heap-owned Expr pointers
};

/// One arm of a CASE WHEN expression.
pub const CaseArm = struct {
    cond: WhereClause, // WHEN <cond>
    result: *Expr, // THEN <expr>
};

/// CASE WHEN c1 THEN e1 WHEN c2 THEN e2 ... [ELSE else_expr] END
pub const CaseExpr = struct {
    arms: []CaseArm, // heap-owned
    else_expr: ?*Expr, // null → emit null when no arm matches
};

// ---- AST -------------------------------------------------------------------

/// Aggregation function for group-by queries
pub const AggFunc = enum { sum, avg, min, max, stddev, variance };

pub const SelectField = struct {
    key: []const u8,
    alias: ?[]const u8,
    /// When non-null this field was written as ISNULL(key, default).
    /// The default is emitted when the field is missing or null.
    default_val: ?[]const u8 = null, // heap-owned string
    /// When non-null, overrides key: the column value is computed by this expression.
    expr: ?*Expr = null, // heap-owned
    /// When non-null, this is a group-by aggregate: sum(field), avg(field), etc.
    agg: ?AggFunc = null,
    /// True when this is an ADD column (from SELECT * ADD expr AS alias).
    /// SELECT * add expr as col — copies all fields then appends this computed column.
    is_add: bool = false,
    /// True when this is a REMOVE directive (from SELECT * REMOVE field1, field2).
    is_remove: bool = false,
    /// True when this is the EXPAND directive (expand array field into rows).
    is_expand: bool = false,
    /// When non-null, this expand uses split(key, delim) — split key by delim then expand.
    expand_split_delim: ?[]const u8 = null,
};

/// Sentinel SelectField meaning "select *" (all fields). Detected by key == "*".
/// Also used for DISTINCT tracking (no extra field).
pub const SELECT_STAR_KEY = "*";

pub const Op = enum {
    eq,
    neq,
    gt,
    lt,
    gte,
    lte,
    like,
    contains,
    not_contains,
    starts_with,
    ends_with,
    in_list,
    not_in_list,
    is_null,
    is_not_null,
    has_key, // has(field) — key exists (even if null)
    not_has_key, // not has(field) / NOT HAS(field)
    matches, // field matches 'regex'
    not_matches, // field not matches 'regex'
};

pub const Value = union(enum) {
    string: []const u8,
    number: f64,
    boolean: bool,
    null_val,
    /// Heap-owned slice of heap-owned strings, used by IN / NOT IN.
    string_list: [][]const u8,
};

pub const LogicOp = enum { @"and", @"or" };

pub const Condition = struct {
    key: []const u8,
    op: Op,
    value: Value,
    /// When non-null, the LHS is a computed expression instead of a bare field.
    lhs_expr: ?*Expr = null, // heap-owned
};

pub const WhereBool = struct { lhs: *WhereClause, rhs: *WhereClause };

pub const WhereClause = union(enum) {
    leaf: Condition,
    and_: WhereBool,
    or_: WhereBool,
    not_: *WhereClause, // NOT (inner)

    pub fn deinit(self: WhereClause, allocator: std.mem.Allocator) void {
        switch (self) {
            .leaf => |c| {
                allocator.free(c.key);
                switch (c.value) {
                    .string => |s| allocator.free(s),
                    .string_list => |sl| {
                        for (sl) |s| allocator.free(s);
                        allocator.free(sl);
                    },
                    else => {},
                }
                if (c.lhs_expr) |e| {
                    e.deinit(allocator);
                    allocator.destroy(e);
                }
            },
            inline .and_, .or_ => |b| {
                b.lhs.deinit(allocator);
                allocator.destroy(b.lhs);
                b.rhs.deinit(allocator);
                allocator.destroy(b.rhs);
            },
            .not_ => |inner| {
                inner.deinit(allocator);
                allocator.destroy(inner);
            },
        }
    }
};

pub const SortDir = enum { asc, desc };

pub const OrderField = struct {
    key: []const u8,
    dir: SortDir,
};

/// One GROUP BY key: either a plain field lookup or an evaluated expression.
pub const GroupByKey = struct {
    key: []const u8, // output field name in the result record
    expr: ?*Expr = null, // if set, evaluate this expression; otherwise r.get(key)
};

pub const Query = struct {
    scope_pattern: ?[]const u8,
    fields: ?[]SelectField,
    where: ?WhereClause,
    having: ?WhereClause = null, // post-GROUP BY filter
    order_by: ?[]OrderField, // null = no sort
    limit: ?usize, // null = no limit
    group_by: ?[]GroupByKey, // group keys (null = no grouping)
    has_count: bool, // true when count() is in SELECT
    global_agg: bool = false, // true when aggregates appear without group by
    distinct: bool = false, // true when DISTINCT keyword in SELECT
    into: ?[]const u8 = null, // INTO 'path' — output destination file

    pub fn deinit(self: Query, allocator: std.mem.Allocator) void {
        if (self.into) |p| allocator.free(p);
        if (self.scope_pattern) |s| allocator.free(s);
        if (self.fields) |fields| {
            for (fields) |f| {
                allocator.free(f.key);
                if (f.alias) |a| allocator.free(a);
                if (f.default_val) |d| allocator.free(d);
                if (f.expr) |e| {
                    e.deinit(allocator);
                    allocator.destroy(e);
                }
            }
            allocator.free(fields);
        }
        if (self.where) |w| w.deinit(allocator);
        if (self.having) |h| h.deinit(allocator);
        if (self.order_by) |ob| {
            for (ob) |f| allocator.free(f.key);
            allocator.free(ob);
        }
        if (self.group_by) |gs| {
            for (gs) |g| {
                allocator.free(g.key);
                if (g.expr) |e| {
                    e.deinit(allocator);
                    allocator.destroy(e);
                }
            }
            allocator.free(gs);
        }
    }
};

// ---- TOKENIZER -------------------------------------------------------------

