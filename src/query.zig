//! kqq query engine
//!
//! Three query modes:
//!
//!   1. FLAT mode  (no brackets)
//!      where users.*.score > 80
//!      select users.*.name, users.*.score
//!      select users.*.name where users.*.score > 80
//!      -> operates key-by-key on the flat map, no record grouping
//!
//!   2. SCOPED mode  [prefix]
//!      [users.*] select name, score where score > 80
//!      [services.*] where cpu_pct > 50
//!      -> groups flat keys by prefix, SELECT/WHERE use relative field names
//!
//!   3. AUTO mode  [*]
//!      [*] select name where score > 80
//!      -> auto-detects all array roots, same relative SELECT/WHERE

const std = @import("std");
const regex = @import("kqq_regex");

const simpleRegexMatch = regex.simpleRegexMatch;
const globLike = regex.globLike;
const ast = @import("kqq_ast");

// Re-export all AST types for backward compatibility
pub const ExprTag = ast.ExprTag;
pub const FuncName = ast.FuncName;
pub const Expr = ast.Expr;
pub const BinExpr = ast.BinExpr;
pub const FuncExpr = ast.FuncExpr;
pub const CaseArm = ast.CaseArm;
pub const CaseExpr = ast.CaseExpr;
pub const AggFunc = ast.AggFunc;
pub const SelectField = ast.SelectField;
pub const SELECT_STAR_KEY = ast.SELECT_STAR_KEY;
pub const Op = ast.Op;
pub const Value = ast.Value;
pub const LogicOp = ast.LogicOp;
pub const Condition = ast.Condition;
pub const WhereBool = ast.WhereBool;
pub const WhereClause = ast.WhereClause;
pub const SortDir = ast.SortDir;
pub const OrderField = ast.OrderField;
pub const GroupByKey = ast.GroupByKey;
pub const Query = ast.Query;
pub const cloneExpr = ast.cloneExpr;

// Tokenizer (from tokenizer.zig)
const tokenizer_mod = @import("kqq_tokenizer");
const TokTag = tokenizer_mod.TokTag;
const Token = tokenizer_mod.Token;
const Tokenizer = tokenizer_mod.Tokenizer;
const isIdentStart = tokenizer_mod.isIdentStart;
const isIdentCont = tokenizer_mod.isIdentCont;
const keywordTag = tokenizer_mod.keywordTag;
const isKeywordIdent = tokenizer_mod.isKeywordIdent;

// ---- PARSER ----------------------------------------------------------------
// ---- PARSER ----------------------------------------------------------------

pub const ParseError = error{ UnexpectedToken, UnexpectedChar, UnknownFunction, OutOfMemory };

fn parseGlobKey(
    allocator: std.mem.Allocator,
    tok: *Tokenizer,
    cur: *Token,
) ParseError![]const u8 {
    var buf = std.array_list.Managed(u8).init(allocator);
    errdefer buf.deinit();

    // Any keyword can also serve as a field name (e.g. `count`, `level`, `in`, `is`)
    const isIdentLike = isKeywordIdent(cur.tag);

    if (cur.tag == .star) {
        try buf.appendSlice("*");
        cur.* = try tok.next();
    } else if (isIdentLike) {
        try buf.appendSlice(cur.text);
        cur.* = try tok.next();
        while (cur.tag == .dot) {
            cur.* = try tok.next();
            if (cur.tag == .star) {
                try buf.appendSlice(".*");
                cur.* = try tok.next();
            } else if (isKeywordIdent(cur.tag)) {
                try buf.appendSlice(".");
                try buf.appendSlice(cur.text);
                cur.* = try tok.next();
            } else return error.UnexpectedToken;
        }
    } else return error.UnexpectedToken;

    return buf.toOwnedSlice();
}

// ---- EXPRESSION PARSER ----------------------------------------------------
//
// Grammar (simplified, left-recursive eliminated via precedence climbing):
//   expr   := term  (('+' | '-') term)*
//   term   := unary (('*' | '/' | '%') unary)*
//   unary  := primary
//   primary:= number | string | func_call | field_ref | '(' expr ')'
//   func_call := FUNC_KW '(' expr (',' expr)* ')'
//   field_ref := ident ('.' ident)*

fn isFuncKeyword(tag: TokTag) ?FuncName {
    return switch (tag) {
        .kw_upper => .upper,
        .kw_lower => .lower,
        .kw_len => .len,
        .kw_round => .round,
        .kw_floor => .floor,
        .kw_ceil => .ceil,
        .kw_trim => .trim,
        .kw_concat => .concat,
        .kw_substr => .substr,
        .kw_abs => .abs,
        .kw_to_str => .to_str,
        .kw_to_number => .to_number,
        .kw_format => .format,
        .kw_coalesce => .coalesce,
        .kw_replace => .replace,
        .kw_lpad => .lpad,
        .kw_rpad => .rpad,
        .kw_split => .split,
        .kw_type => .type_of,
        .kw_keys => .keys,
        .kw_values => .values,
        .kw_to_entries => .to_entries,
        .kw_now => .now,
        .kw_now_epoch => .now_epoch,
        .kw_now_ms => .now_ms,
        .kw_from_epoch => .from_epoch,
        .kw_from_epoch_ms => .from_epoch_ms,
        .kw_to_epoch => .to_epoch,
        .kw_to_epoch_ms => .to_epoch_ms,
        .kw_date_part => .date_part,
        .kw_epoch_min => .epoch_min,
        .kw_epoch_hour => .epoch_hour,
        .kw_epoch_day => .epoch_day,
        .kw_epoch_week => .epoch_week,
        else => null,
    };
}

/// Map a function name (case-insensitive) to a FuncName. Covers names that
/// are not keywords too (`ifhas`), so `ident(` can be resolved as a call.
fn funcNameFromStr(s: []const u8) ?FuncName {
    const names = .{
        .{ "upper", FuncName.upper },                 .{ "lower", FuncName.lower },
        .{ "len", FuncName.len },                     .{ "round", FuncName.round },
        .{ "floor", FuncName.floor },                 .{ "ceil", FuncName.ceil },
        .{ "trim", FuncName.trim },                   .{ "concat", FuncName.concat },
        .{ "substr", FuncName.substr },               .{ "abs", FuncName.abs },
        .{ "to_str", FuncName.to_str },               .{ "to_number", FuncName.to_number },
        .{ "format", FuncName.format },               .{ "coalesce", FuncName.coalesce },
        .{ "replace", FuncName.replace },             .{ "lpad", FuncName.lpad },
        .{ "rpad", FuncName.rpad },                   .{ "split", FuncName.split },
        .{ "type", FuncName.type_of },                .{ "keys", FuncName.keys },
        .{ "values", FuncName.values },               .{ "to_entries", FuncName.to_entries },
        .{ "now", FuncName.now },                     .{ "now_epoch", FuncName.now_epoch },
        .{ "now_ms", FuncName.now_ms },               .{ "from_epoch", FuncName.from_epoch },
        .{ "from_epoch_ms", FuncName.from_epoch_ms }, .{ "to_epoch", FuncName.to_epoch },
        .{ "to_epoch_ms", FuncName.to_epoch_ms },     .{ "date_part", FuncName.date_part },
        .{ "epoch_min", FuncName.epoch_min },         .{ "epoch_hour", FuncName.epoch_hour },
        .{ "epoch_day", FuncName.epoch_day },         .{ "epoch_week", FuncName.epoch_week },
        .{ "ifhas", FuncName.ifhas },
    };
    inline for (names) |pair| {
        if (std.ascii.eqlIgnoreCase(s, pair[0])) return pair[1];
    }
    return null;
}

fn destroyExpr(allocator: std.mem.Allocator, e: *Expr) void {
    e.deinit(allocator);
    allocator.destroy(e);
}

/// Build `lhs <op> rhs`, taking ownership of both operands (freed on failure).
fn newBinary(allocator: std.mem.Allocator, op: TokTag, lhs: *Expr, rhs: *Expr) ParseError!*Expr {
    const node = allocator.create(Expr) catch {
        destroyExpr(allocator, lhs);
        destroyExpr(allocator, rhs);
        return error.OutOfMemory;
    };
    const bin = BinExpr{ .lhs = lhs, .rhs = rhs };
    node.* = switch (op) {
        .op_plus => .{ .add = bin },
        .op_minus => .{ .sub = bin },
        .star => .{ .mul = bin },
        .op_div => .{ .div = bin },
        .op_mod => .{ .mod = bin },
        else => unreachable,
    };
    return node;
}

/// Build coalesce(lhs, rhs) — the desugaring of `lhs // rhs`.
fn newCoalesce(allocator: std.mem.Allocator, lhs: *Expr, rhs: *Expr) ParseError!*Expr {
    const args = allocator.alloc(*Expr, 2) catch {
        destroyExpr(allocator, lhs);
        destroyExpr(allocator, rhs);
        return error.OutOfMemory;
    };
    const node = allocator.create(Expr) catch {
        allocator.free(args);
        destroyExpr(allocator, lhs);
        destroyExpr(allocator, rhs);
        return error.OutOfMemory;
    };
    args[0] = lhs;
    args[1] = rhs;
    node.* = .{ .func = .{ .name = .coalesce, .args = args } };
    return node;
}

/// A negative number token right after an operand (`a -1`): the tokenizer
/// glued the minus to the digits, but it is a subtraction.
fn isGluedMinus(t: Token) bool {
    return t.tag == .number and t.text.len > 1 and t.text[0] == '-';
}

/// True if `t` continues an expression after an operand.
fn isArithOp(t: Token) bool {
    return switch (t.tag) {
        .op_plus, .op_minus, .star, .op_div, .op_mod, .op_alt => true,
        else => isGluedMinus(t),
    };
}

fn parseExpr(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!*Expr {
    return parseExprFrom(allocator, tok, cur, try parsePrimary(allocator, tok, cur));
}

/// Finish an expression whose leftmost operand `first` is already parsed.
/// Precedence: `* / %` bind tighter than `+ -`, which bind tighter than `//`.
/// Takes ownership of `first` (freed on error).
fn parseExprFrom(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token, first: *Expr) ParseError!*Expr {
    var lhs = try parseAddFrom(allocator, tok, cur, first);
    // // (alternative) has lower precedence than arithmetic; desugar to coalesce(lhs, rhs)
    while (cur.tag == .op_alt) {
        cur.* = tok.next() catch |e| {
            destroyExpr(allocator, lhs);
            return e;
        };
        const rhs = parseAdd(allocator, tok, cur) catch |e| {
            destroyExpr(allocator, lhs);
            return e;
        };
        lhs = try newCoalesce(allocator, lhs, rhs);
    }
    return lhs;
}

fn parseAdd(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!*Expr {
    return parseAddFrom(allocator, tok, cur, try parsePrimary(allocator, tok, cur));
}

fn parseAddFrom(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token, first: *Expr) ParseError!*Expr {
    var lhs = try parseMulFrom(allocator, tok, cur, first);
    while (cur.tag == .op_plus or cur.tag == .op_minus or isGluedMinus(cur.*)) {
        // `a -1`: keep the (negative) literal as the right operand of an addition.
        const op: TokTag = if (cur.tag == .number) .op_plus else cur.tag;
        if (cur.tag != .number) {
            cur.* = tok.next() catch |e| {
                destroyExpr(allocator, lhs);
                return e;
            };
        }
        const rhs = parseMul(allocator, tok, cur) catch |e| {
            destroyExpr(allocator, lhs);
            return e;
        };
        lhs = try newBinary(allocator, op, lhs, rhs);
    }
    return lhs;
}

fn parseMul(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!*Expr {
    return parseMulFrom(allocator, tok, cur, try parsePrimary(allocator, tok, cur));
}

fn parseMulFrom(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token, first: *Expr) ParseError!*Expr {
    var lhs = first;
    while (cur.tag == .star or cur.tag == .op_div or cur.tag == .op_mod) {
        const op = cur.tag;
        cur.* = tok.next() catch |e| {
            destroyExpr(allocator, lhs);
            return e;
        };
        const rhs = parsePrimary(allocator, tok, cur) catch |e| {
            destroyExpr(allocator, lhs);
            return e;
        };
        lhs = try newBinary(allocator, op, lhs, rhs);
    }
    return lhs;
}

/// Parse `( args... )` of a call to `name`; `cur` is at the '('.
fn parseCall(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token, name: FuncName) ParseError!*Expr {
    if (cur.tag != .lparen) return error.UnexpectedToken;
    cur.* = try tok.next();
    var args = std.array_list.Managed(*Expr).init(allocator);
    errdefer {
        for (args.items) |a| destroyExpr(allocator, a);
        args.deinit();
    }
    if (cur.tag != .rparen) {
        while (true) {
            const arg = try parseExpr(allocator, tok, cur);
            args.append(arg) catch {
                destroyExpr(allocator, arg);
                return error.OutOfMemory;
            };
            if (cur.tag != .comma) break;
            cur.* = try tok.next();
        }
    }
    if (cur.tag != .rparen) return error.UnexpectedToken;
    cur.* = try tok.next();
    const node = try allocator.create(Expr);
    errdefer allocator.destroy(node);
    node.* = .{ .func = .{ .name = name, .args = try args.toOwnedSlice() } };
    return node;
}

/// Resolve `name(` to a function, or fail with UnknownFunction pointing at the name.
fn lookupFunction(tok: *Tokenizer, name_tok: Token) ParseError!FuncName {
    return funcNameFromStr(name_tok.text) orelse {
        tok.err_pos = name_tok.start;
        return error.UnknownFunction;
    };
}

fn parsePrimary(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!*Expr {
    switch (cur.tag) {
        .number => {
            const n = std.fmt.parseFloat(f64, cur.text) catch return error.UnexpectedToken;
            cur.* = try tok.next();
            const node = try allocator.create(Expr);
            node.* = .{ .lit_num = n };
            return node;
        },
        .string => {
            const s = try allocator.dupe(u8, cur.text);
            errdefer allocator.free(s);
            cur.* = try tok.next();
            const node = try allocator.create(Expr);
            node.* = .{ .lit_str = s };
            return node;
        },
        // Grouped expression
        .lparen => {
            cur.* = try tok.next();
            const inner = try parseExpr(allocator, tok, cur);
            if (cur.tag != .rparen) {
                destroyExpr(allocator, inner);
                return error.UnexpectedToken;
            }
            cur.* = tok.next() catch |e| {
                destroyExpr(allocator, inner);
                return e;
            };
            return inner;
        },
        // CASE WHEN c1 THEN e1 [WHEN c2 THEN e2 ...] [ELSE eN] END
        .kw_case => {
            cur.* = try tok.next();
            var arms = std.array_list.Managed(CaseArm).init(allocator);
            errdefer {
                for (arms.items) |*arm| {
                    arm.cond.deinit(allocator);
                    destroyExpr(allocator, arm.result);
                }
                arms.deinit();
            }
            var else_expr: ?*Expr = null;
            errdefer if (else_expr) |e| destroyExpr(allocator, e);
            while (cur.tag == .kw_when) {
                cur.* = try tok.next();
                const arm_cond = try parseWhereOr(allocator, tok, cur);
                errdefer arm_cond.deinit(allocator);
                if (cur.tag != .kw_then) return error.UnexpectedToken;
                cur.* = try tok.next();
                const result_expr = try parseExpr(allocator, tok, cur);
                arms.append(.{ .cond = arm_cond, .result = result_expr }) catch {
                    destroyExpr(allocator, result_expr);
                    return error.OutOfMemory;
                };
            }
            if (arms.items.len == 0) return error.UnexpectedToken;
            if (cur.tag == .kw_else) {
                cur.* = try tok.next();
                else_expr = try parseExpr(allocator, tok, cur);
            }
            if (cur.tag != .kw_end) return error.UnexpectedToken;
            cur.* = try tok.next();
            const node = try allocator.create(Expr);
            errdefer allocator.destroy(node);
            node.* = .{ .case_when = .{ .arms = try arms.toOwnedSlice(), .else_expr = else_expr } };
            return node;
        },
        else => {},
    }
    // A name: a function call when followed by '(', otherwise a field
    // reference `seg('.' seg)*`. Any keyword can name a field (`count`, `type`).
    if (!isKeywordIdent(cur.tag)) return error.UnexpectedToken;
    const name_tok = cur.*;
    cur.* = try tok.next();
    if (cur.tag == .lparen) {
        const fn_name = isFuncKeyword(name_tok.tag) orelse try lookupFunction(tok, name_tok);
        return parseCall(allocator, tok, cur, fn_name);
    }
    var buf = std.array_list.Managed(u8).init(allocator);
    errdefer buf.deinit();
    try buf.appendSlice(name_tok.text);
    while (cur.tag == .dot) {
        cur.* = try tok.next();
        if (!isKeywordIdent(cur.tag) or cur.tag == .number) return error.UnexpectedToken;
        try buf.append('.');
        try buf.appendSlice(cur.text);
        cur.* = try tok.next();
    }
    const field_name = try buf.toOwnedSlice();
    errdefer allocator.free(field_name);
    const node = try allocator.create(Expr);
    node.* = .{ .field = field_name };
    return node;
}

// ---- WHERE BOOLEAN TREE PARSER ---------------------------------------------
//
// Grammar (standard SQL precedence):
//   where_expr := where_or
//   where_or   := where_and  ('or'  where_and)*
//   where_and  := where_prim ('and' where_prim)*
//   where_prim := '(' where_or ')'  |  condition_leaf
//
// AND binds tighter than OR; parentheses override.

fn isComparisonOp(tag: TokTag) bool {
    return switch (tag) {
        .op_eq, .op_neq, .op_gt, .op_lt, .op_gte, .op_lte, .kw_like, .kw_is, .kw_not, .kw_in, .kw_contains, .kw_starts_with, .kw_ends_with, .kw_matches, .kw_between => true,
        else => false,
    };
}

/// Parse the operator + value part of a condition leaf, given an already-resolved
/// key and optional LHS expression.  Ownership of `key` and `lhs_expr` is
/// transferred on success (errdefer frees them on failure).
fn parseConditionBody(
    allocator: std.mem.Allocator,
    tok: *Tokenizer,
    cur: *Token,
    key: []const u8,
    lhs_expr: ?*Expr,
) ParseError!WhereClause {
    errdefer allocator.free(key);
    errdefer if (lhs_expr) |e| {
        e.deinit(allocator);
        allocator.destroy(e);
    };

    var op: Op = undefined;
    var val: Value = .null_val;

    if (cur.tag == .kw_is) {
        cur.* = try tok.next();
        if (cur.tag == .kw_not) {
            cur.* = try tok.next();
            if (cur.tag != .kw_null) return error.UnexpectedToken;
            op = .is_not_null;
        } else if (cur.tag == .kw_null) {
            op = .is_null;
        } else return error.UnexpectedToken;
        cur.* = try tok.next();
    } else if (cur.tag == .kw_not) {
        cur.* = try tok.next();
        if (cur.tag == .kw_in) {
            op = .not_in_list;
            cur.* = try tok.next();
            if (cur.tag != .lparen) return error.UnexpectedToken;
            cur.* = try tok.next();
            var items = std.array_list.Managed([]const u8).init(allocator);
            errdefer {
                for (items.items) |s| allocator.free(s);
                items.deinit();
            }
            while (cur.tag == .string or cur.tag == .number) {
                try items.append(try allocator.dupe(u8, cur.text));
                cur.* = try tok.next();
                if (cur.tag == .comma) cur.* = try tok.next();
            }
            if (cur.tag != .rparen) return error.UnexpectedToken;
            cur.* = try tok.next();
            val = .{ .string_list = try items.toOwnedSlice() };
        } else if (cur.tag == .kw_contains) {
            op = .not_contains;
            cur.* = try tok.next();
            if (cur.tag != .string) return error.UnexpectedToken;
            val = .{ .string = try allocator.dupe(u8, cur.text) };
            cur.* = try tok.next();
        } else if (cur.tag == .kw_matches) {
            op = .not_matches;
            cur.* = try tok.next();
            if (cur.tag != .string) return error.UnexpectedToken;
            val = .{ .string = try allocator.dupe(u8, cur.text) };
            cur.* = try tok.next();
        } else return error.UnexpectedToken;
    } else if (cur.tag == .kw_in) {
        op = .in_list;
        cur.* = try tok.next();
        if (cur.tag != .lparen) return error.UnexpectedToken;
        cur.* = try tok.next();
        var items = std.array_list.Managed([]const u8).init(allocator);
        errdefer {
            for (items.items) |s| allocator.free(s);
            items.deinit();
        }
        while (cur.tag == .string or cur.tag == .number) {
            try items.append(try allocator.dupe(u8, cur.text));
            cur.* = try tok.next();
            if (cur.tag == .comma) cur.* = try tok.next();
        }
        if (cur.tag != .rparen) return error.UnexpectedToken;
        cur.* = try tok.next();
        val = .{ .string_list = try items.toOwnedSlice() };
    } else if (cur.tag == .kw_contains) {
        op = .contains;
        cur.* = try tok.next();
        if (cur.tag != .string) return error.UnexpectedToken;
        val = .{ .string = try allocator.dupe(u8, cur.text) };
        cur.* = try tok.next();
    } else if (cur.tag == .kw_starts_with) {
        op = .starts_with;
        cur.* = try tok.next();
        if (cur.tag != .string) return error.UnexpectedToken;
        val = .{ .string = try allocator.dupe(u8, cur.text) };
        cur.* = try tok.next();
    } else if (cur.tag == .kw_ends_with) {
        op = .ends_with;
        cur.* = try tok.next();
        if (cur.tag != .string) return error.UnexpectedToken;
        val = .{ .string = try allocator.dupe(u8, cur.text) };
        cur.* = try tok.next();
    } else if (cur.tag == .kw_matches) {
        op = .matches;
        cur.* = try tok.next();
        if (cur.tag != .string) return error.UnexpectedToken;
        val = .{ .string = try allocator.dupe(u8, cur.text) };
        cur.* = try tok.next();
    } else if (cur.tag == .kw_between) {
        // BETWEEN with explicit interval notation:
        //   [lo and hi]  →  field >= lo and field <= hi  (both inclusive)
        //   (lo and hi)  →  field >  lo and field <  hi  (both exclusive)
        //   [lo and hi)  →  field >= lo and field <  hi  (left-inc, right-exc)
        //   (lo and hi]  →  field >  lo and field <= hi  (left-exc, right-inc)
        cur.* = try tok.next();
        const lo_inclusive = switch (cur.tag) {
            .lbracket => true,
            .lparen => false,
            else => return error.UnexpectedToken,
        };
        cur.* = try tok.next();
        const lo_val: Value = switch (cur.tag) {
            .number => .{ .number = std.fmt.parseFloat(f64, cur.text) catch return error.UnexpectedToken },
            .string => .{ .string = try allocator.dupe(u8, cur.text) },
            else => return error.UnexpectedToken,
        };
        cur.* = try tok.next();
        if (cur.tag != .kw_and) return error.UnexpectedToken;
        cur.* = try tok.next();
        const hi_val: Value = switch (cur.tag) {
            .number => .{ .number = std.fmt.parseFloat(f64, cur.text) catch return error.UnexpectedToken },
            .string => .{ .string = try allocator.dupe(u8, cur.text) },
            else => return error.UnexpectedToken,
        };
        cur.* = try tok.next();
        const hi_inclusive = switch (cur.tag) {
            .rbracket => true,
            .rparen => false,
            else => return error.UnexpectedToken,
        };
        cur.* = try tok.next();
        const lo_op: Op = if (lo_inclusive) .gte else .gt;
        const hi_op: Op = if (hi_inclusive) .lte else .lt;
        // Build the AND of two leaf conditions; key is owned by lo_ptr, key_hi by hi_ptr.
        const key_hi = try allocator.dupe(u8, key);
        const lo_ptr = try allocator.create(WhereClause);
        errdefer allocator.destroy(lo_ptr);
        const hi_ptr = try allocator.create(WhereClause);
        lo_ptr.* = WhereClause{ .leaf = .{ .key = key, .op = lo_op, .value = lo_val, .lhs_expr = lhs_expr } };
        hi_ptr.* = WhereClause{ .leaf = .{ .key = key_hi, .op = hi_op, .value = hi_val, .lhs_expr = null } };
        return WhereClause{ .and_ = .{ .lhs = lo_ptr, .rhs = hi_ptr } };
    } else {
        op = switch (cur.tag) {
            .op_eq => .eq,
            .op_neq => .neq,
            .op_gt => .gt,
            .op_lt => .lt,
            .op_gte => .gte,
            .op_lte => .lte,
            .kw_like => .like,
            else => return error.UnexpectedToken,
        };
        cur.* = try tok.next();
        val = switch (cur.tag) {
            .string => .{ .string = try allocator.dupe(u8, cur.text) },
            .number => .{ .number = std.fmt.parseFloat(f64, cur.text) catch return error.UnexpectedToken },
            .kw_true => .{ .boolean = true },
            .kw_false => .{ .boolean = false },
            .kw_null => .null_val,
            else => return error.UnexpectedToken,
        };
        cur.* = try tok.next();
    }
    return WhereClause{ .leaf = .{ .key = key, .op = op, .value = val, .lhs_expr = lhs_expr } };
}

fn parseWherePrimary(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!WhereClause {
    // '(' can be either:
    //   a) a grouped WHERE expression: '(' where_or ')'
    //   b) an arithmetic LHS:          '(' expr ')' comparison_op value
    // Disambiguate by speculatively trying parseExpr; if a comparison op
    // follows the closed paren, it is case (b), otherwise restore and use (a).
    if (cur.tag == .lparen) {
        const tok_save = tok.*;
        const cur_save = cur.*;
        if (parseExpr(allocator, tok, cur)) |arith_lhs| {
            if (isComparisonOp(cur.tag)) {
                const key2 = try allocator.dupe(u8, "__expr__");
                return parseConditionBody(allocator, tok, cur, key2, arith_lhs);
            }
            arith_lhs.deinit(allocator);
            allocator.destroy(arith_lhs);
        } else |_| {}
        // Not arithmetic – restore and treat as WHERE group.
        tok.* = tok_save;
        cur.* = cur_save;
        cur.* = try tok.next();
        const inner = try parseWhereOr(allocator, tok, cur);
        errdefer inner.deinit(allocator);
        if (cur.tag != .rparen) return error.UnexpectedToken;
        cur.* = try tok.next();
        return inner;
    }

    // Single condition leaf.
    if (cur.tag == .kw_has) {
        cur.* = try tok.next();
        const negate = cur.tag == .kw_not;
        if (negate) cur.* = try tok.next();
        if (cur.tag != .lparen) return error.UnexpectedToken;
        cur.* = try tok.next();
        const key = try parseGlobKey(allocator, tok, cur);
        if (cur.tag != .rparen) {
            allocator.free(key);
            return error.UnexpectedToken;
        }
        cur.* = tok.next() catch |e| {
            allocator.free(key);
            return e;
        };
        return WhereClause{ .leaf = .{ .key = key, .op = if (negate) .not_has_key else .has_key, .value = .null_val, .lhs_expr = null } };
    }

    if (cur.tag == .kw_not) {
        var tok_peek = tok.*;
        const peek_tok = try tok_peek.next();
        if (peek_tok.tag == .kw_has) {
            cur.* = try tok.next(); // consume 'not'
            cur.* = try tok.next(); // consume 'has'
            if (cur.tag != .lparen) return error.UnexpectedToken;
            cur.* = try tok.next();
            const key = try parseGlobKey(allocator, tok, cur);
            if (cur.tag != .rparen) {
                allocator.free(key);
                return error.UnexpectedToken;
            }
            cur.* = tok.next() catch |e| {
                allocator.free(key);
                return e;
            };
            return WhereClause{ .leaf = .{ .key = key, .op = .not_has_key, .value = .null_val, .lhs_expr = null } };
        }
        if (peek_tok.tag == .lparen) {
            // NOT ( where_expr ) — wrap inner clause in not_
            cur.* = try tok.next(); // consume 'not'
            cur.* = try tok.next(); // consume '('
            const inner = try parseWhereOr(allocator, tok, cur);
            errdefer inner.deinit(allocator);
            if (cur.tag != .rparen) return error.UnexpectedToken;
            cur.* = try tok.next();
            const inner_ptr = try allocator.create(WhereClause);
            inner_ptr.* = inner;
            return WhereClause{ .not_ = inner_ptr };
        }
        // fall through — NOT IN / NOT CONTAINS / NOT MATCHES handled in parseConditionBody
    }

    // A call (`len(name) > 5`, `ifhas(...)`) is a computed LHS. A function
    // keyword not followed by '(' is a plain field name (`where keys = "x"`).
    if (isFuncKeyword(cur.tag) != null or cur.tag == .ident) {
        var tok_peek = tok.*;
        const peek = tok_peek.next() catch return error.UnexpectedToken;
        if (peek.tag == .lparen) {
            const e = try parseExpr(allocator, tok, cur);
            const k = allocator.dupe(u8, "__expr__") catch {
                destroyExpr(allocator, e);
                return error.OutOfMemory;
            };
            return parseConditionBody(allocator, tok, cur, k, e);
        }
    }

    const key = try parseGlobKey(allocator, tok, cur);

    // Arithmetic after the field makes it a computed LHS:
    // `where price * qty > 100`, `where score // 0 > 50`.
    if (isArithOp(cur.*)) {
        const field_node = allocator.create(Expr) catch {
            allocator.free(key);
            return error.OutOfMemory;
        };
        field_node.* = .{ .field = key }; // key ownership transferred into Expr
        const e = try parseExprFrom(allocator, tok, cur, field_node);
        const k = allocator.dupe(u8, "__expr__") catch {
            destroyExpr(allocator, e);
            return error.OutOfMemory;
        };
        return parseConditionBody(allocator, tok, cur, k, e);
    }

    return parseConditionBody(allocator, tok, cur, key, null);
}

fn parseWhereAnd(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!WhereClause {
    var lhs = try parseWherePrimary(allocator, tok, cur);
    while (cur.tag == .kw_and) {
        cur.* = tok.next() catch |e| {
            lhs.deinit(allocator);
            return e;
        };
        const rhs = parseWherePrimary(allocator, tok, cur) catch |e| {
            lhs.deinit(allocator);
            return e;
        };
        const l = allocator.create(WhereClause) catch {
            lhs.deinit(allocator);
            rhs.deinit(allocator);
            return error.OutOfMemory;
        };
        const r = allocator.create(WhereClause) catch {
            lhs.deinit(allocator);
            rhs.deinit(allocator);
            allocator.destroy(l);
            return error.OutOfMemory;
        };
        l.* = lhs;
        r.* = rhs;
        lhs = WhereClause{ .and_ = .{ .lhs = l, .rhs = r } };
    }
    return lhs;
}

fn parseWhereOr(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!WhereClause {
    var lhs = try parseWhereAnd(allocator, tok, cur);
    while (cur.tag == .kw_or) {
        cur.* = tok.next() catch |e| {
            lhs.deinit(allocator);
            return e;
        };
        const rhs = parseWhereAnd(allocator, tok, cur) catch |e| {
            lhs.deinit(allocator);
            return e;
        };
        const l = allocator.create(WhereClause) catch {
            lhs.deinit(allocator);
            rhs.deinit(allocator);
            return error.OutOfMemory;
        };
        const r = allocator.create(WhereClause) catch {
            lhs.deinit(allocator);
            rhs.deinit(allocator);
            allocator.destroy(l);
            return error.OutOfMemory;
        };
        l.* = lhs;
        r.* = rhs;
        lhs = WhereClause{ .or_ = .{ .lhs = l, .rhs = r } };
    }
    return lhs;
}

// ---- MAIN PARSER -----------------------------------------------------------

/// Parse a GROUP BY clause (assumes `cur` is at `kw_group`).
/// `fields` is the already-parsed SELECT field list (may be null if GROUP BY
/// appears before SELECT) — used for alias resolution.
fn parseGroupByClause(
    allocator: std.mem.Allocator,
    tok: *Tokenizer,
    cur: *Token,
    fields: ?[]const SelectField,
) ParseError![]GroupByKey {
    cur.* = try tok.next(); // consume 'group'
    if (cur.tag != .kw_by) return error.UnexpectedToken;
    cur.* = try tok.next(); // consume 'by'
    var gkeys = std.array_list.Managed(GroupByKey).init(allocator);
    errdefer {
        for (gkeys.items) |*g| {
            allocator.free(g.key);
            if (g.expr) |e| {
                e.deinit(allocator);
                allocator.destroy(e);
            }
        }
        gkeys.deinit();
    }
    while (true) {
        // Parse the GROUP BY term as a full expression so we can handle
        // function calls (e.g. `group by date_part(ts, "year")`) and
        // SELECT alias resolution (e.g. `group by yr` where `yr = date_part(...)`).
        const e = try parseExpr(allocator, tok, cur);
        const gk: GroupByKey = switch (e.*) {
            .field => |fname| blk: {
                // Check if fname matches a SELECT field's alias or key.
                // Expression fields: clone the expr and name the group key by alias/key.
                // Plain aliased fields (e.g. `select dept as d ... group by d`): group on
                // the real field via an expr, and name the output column by the alias.
                if (fields) |flds| {
                    for (flds) |f| {
                        if (f.agg != null or f.is_remove or f.is_expand) continue;
                        const alias_match = f.alias != null and std.mem.eql(u8, f.alias.?, fname);
                        const key_match = f.alias == null and std.mem.eql(u8, f.key, fname);
                        if (!alias_match and !key_match) continue;
                        const out_key = try allocator.dupe(u8, if (f.alias) |a| a else f.key);
                        errdefer allocator.free(out_key);
                        // Free the trivial .field expr we just parsed
                        e.deinit(allocator);
                        allocator.destroy(e);
                        if (f.expr) |fe| {
                            const cloned = try cloneExpr(allocator, fe);
                            break :blk .{ .key = out_key, .expr = cloned };
                        }
                        if (alias_match) {
                            // Plain aliased field: evaluate the real field via an expr.
                            const fe = try allocator.create(Expr);
                            errdefer {
                                fe.deinit(allocator);
                                allocator.destroy(fe);
                            }
                            fe.* = .{ .field = try allocator.dupe(u8, f.key) };
                            break :blk .{ .key = out_key, .expr = fe };
                        }
                        break :blk .{ .key = out_key, .expr = null };
                    }
                }
                // Plain field lookup — no expression needed.
                const key_copy = try allocator.dupe(u8, fname);
                e.deinit(allocator);
                allocator.destroy(e);
                break :blk .{ .key = key_copy, .expr = null };
            },
            .func => |f| blk: {
                // Inline function call in GROUP BY.
                // Try to find a matching SELECT field by function name → use its alias.
                const fn_name = @tagName(f.name);
                var out_key: []const u8 = fn_name;
                if (fields) |flds| {
                    for (flds) |sf| {
                        if (sf.expr == null or sf.agg != null) continue;
                        if (std.mem.eql(u8, sf.key, fn_name)) {
                            out_key = if (sf.alias) |a| a else sf.key;
                            break;
                        }
                    }
                }
                break :blk .{ .key = try allocator.dupe(u8, out_key), .expr = e };
            },
            else => blk: {
                // Arithmetic or other expression: synthesize a positional key name.
                const syn = try std.fmt.allocPrint(allocator, "__gb_{d}", .{gkeys.items.len});
                break :blk .{ .key = syn, .expr = e };
            },
        };
        try gkeys.append(gk);
        if (cur.tag == .comma) {
            cur.* = try tok.next();
        } else break;
    }
    return try gkeys.toOwnedSlice();
}

/// Rewrite WHERE condition keys that reference SELECT aliases into the real
/// field names (or expressions) they alias. Applied at parse time so every
/// execution path (streaming, raw pushdown, scoped, flat, native) sees the
/// resolved keys. E.g. `select dept as d where d = "eng"` → key becomes `dept`.
fn resolveWhereAliases(
    allocator: std.mem.Allocator,
    w: *WhereClause,
    fields: []const SelectField,
) ParseError!void {
    switch (w.*) {
        .leaf => |*c| {
            // Skip expression LHS conditions — their key is a synthetic "__expr__".
            if (c.lhs_expr != null) return;
            for (fields) |f| {
                if (f.agg != null or f.is_remove or f.is_expand) continue;
                const is_alias = f.alias != null and std.mem.eql(u8, f.alias.?, c.key);
                if (!is_alias) continue;
                // Alias matched: replace key with the real field name.
                const new_key = try allocator.dupe(u8, f.key);
                allocator.free(c.key);
                c.key = new_key;
                return;
            }
        },
        .and_, .or_ => |*b| {
            try resolveWhereAliases(allocator, b.lhs, fields);
            try resolveWhereAliases(allocator, b.rhs, fields);
        },
        .not_ => |inner| try resolveWhereAliases(allocator, inner, fields),
    }
}

/// Parse a query. Keywords are case-insensitive; field names are not.
/// After the optional scope, GROUP BY may precede SELECT; the remaining
/// clauses (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, INTO) may come in any
/// order, each at most once. Anything left unparsed is an error — a query is
/// never silently truncated.
pub fn parse(allocator: std.mem.Allocator, src: []const u8, fail_pos: ?*usize) ParseError!Query {
    var tok = Tokenizer{ .src = src };
    // On any parse error, record where it happened for diagnostics.
    errdefer if (fail_pos) |fp| {
        fp.* = tok.err_pos orelse tok.pos;
    };
    var q = Query{
        .scope_pattern = null,
        .fields = null,
        .where = null,
        .order_by = null,
        .limit = null,
        .group_by = null,
        .has_count = false,
    };
    errdefer q.deinit(allocator);
    var cur = try tok.next();

    // SCOPE [pattern]
    if (cur.tag == .lbracket) {
        cur = try tok.next();
        q.scope_pattern = try parseGlobKey(allocator, &tok, &cur);
        if (cur.tag != .rbracket) return error.UnexpectedToken;
        cur = try tok.next();
    }

    // GROUP BY before SELECT (no SELECT aliases to resolve yet)
    if (cur.tag == .kw_group) q.group_by = try parseGroupByClause(allocator, &tok, &cur, null);

    if (cur.tag == .kw_select) {
        cur = try tok.next();
        try parseSelectList(allocator, &tok, &cur, &q);
    }

    while (cur.tag != .eof) {
        const repeated = switch (cur.tag) {
            .kw_where => q.where != null,
            .kw_group => q.group_by != null,
            .kw_having => q.having != null,
            .kw_order => q.order_by != null,
            .kw_limit => q.limit != null,
            .kw_into => q.into != null,
            else => true, // not a clause keyword: unparsed trailing input
        };
        if (repeated) {
            tok.err_pos = cur.start;
            return error.UnexpectedToken;
        }
        switch (cur.tag) {
            .kw_where => {
                cur = try tok.next();
                q.where = try parseWhereOr(allocator, &tok, &cur);
            },
            .kw_group => q.group_by = try parseGroupByClause(allocator, &tok, &cur, q.fields),
            // HAVING filters groups after aggregation, on output column names.
            .kw_having => {
                cur = try tok.next();
                q.having = try parseWhereOr(allocator, &tok, &cur);
            },
            .kw_order => q.order_by = try parseOrderBy(allocator, &tok, &cur, q.fields),
            .kw_limit => {
                cur = try tok.next();
                if (cur.tag != .number) return error.UnexpectedToken;
                q.limit = std.fmt.parseInt(usize, cur.text, 10) catch return error.UnexpectedToken;
                cur = try tok.next();
            },
            // INTO 'path' — output destination file
            .kw_into => {
                cur = try tok.next();
                if (cur.tag != .string) return error.UnexpectedToken;
                q.into = try allocator.dupe(u8, cur.text);
                cur = try tok.next();
            },
            else => unreachable,
        }
    }

    // Aggregates without GROUP BY aggregate over the whole input.
    if (q.group_by == null) {
        q.global_agg = q.has_count;
        if (q.fields) |flds| for (flds) |f| {
            if (f.agg != null) q.global_agg = true;
        };
    }

    // Resolve SELECT aliases referenced in WHERE (e.g. `select dept as d where d = "x"`).
    // Skipped for HAVING, which intentionally references output column names.
    if (q.fields) |flds| {
        if (q.where) |*w| try resolveWhereAliases(allocator, w, flds);
    }
    return q;
}

/// `as <name>`: `cur` is just past `as`. A keyword (`as count`) or a quoted
/// string (`as "total amount"`) may serve as the name.
fn parseAlias(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError![]const u8 {
    if (cur.tag == .number or (cur.tag != .string and !isKeywordIdent(cur.tag))) return error.UnexpectedToken;
    const alias = try allocator.dupe(u8, cur.text);
    errdefer allocator.free(alias);
    cur.* = try tok.next();
    return alias;
}

/// Optional `as <name>`.
fn parseOptAlias(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!?[]const u8 {
    if (cur.tag != .kw_as) return null;
    cur.* = try tok.next();
    return try parseAlias(allocator, tok, cur);
}

/// Mandatory `as <name>` (computed columns need a name).
fn parseReqAlias(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError![]const u8 {
    return try parseOptAlias(allocator, tok, cur) orelse error.UnexpectedToken;
}

/// The argument of an aggregate, `cur` just past '('. A bare field path
/// (`sum(a.b)`, including keyword names like `sum(count)`) becomes the
/// field's key; anything else (`sum(price * qty)`) is kept as an expression,
/// with a synthetic key. The closing ')' is consumed.
const AggArg = struct { key: []const u8, expr: ?*Expr };

fn parseAggArg(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!AggArg {
    const tok_save = tok.*;
    const cur_save = cur.*;
    if (isKeywordIdent(cur.tag) and cur.tag != .number) {
        if (parseGlobKey(allocator, tok, cur)) |key| {
            if (cur.tag == .rparen) {
                cur.* = tok.next() catch |e| {
                    allocator.free(key);
                    return e;
                };
                return .{ .key = key, .expr = null };
            }
            allocator.free(key);
        } else |_| {}
        tok.* = tok_save;
        cur.* = cur_save;
    }
    const e = try parseExpr(allocator, tok, cur);
    errdefer destroyExpr(allocator, e);
    if (cur.tag != .rparen) return error.UnexpectedToken;
    cur.* = try tok.next();
    return .{ .key = try allocator.dupe(u8, "__agg_expr__"), .expr = e };
}

fn aggFuncOf(tag: TokTag) ?AggFunc {
    return switch (tag) {
        .kw_sum => .sum,
        .kw_avg => .avg,
        .kw_min => .min,
        .kw_max => .max,
        .kw_stddev => .stddev,
        .kw_variance => .variance,
        else => null,
    };
}

/// Parse the SELECT list into `q.fields` (cur is just past `select`).
fn parseSelectList(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token, q: *Query) ParseError!void {
    // Optional DISTINCT after SELECT
    if (cur.tag == .kw_distinct) {
        q.distinct = true;
        cur.* = try tok.next();
    }
    var list = std.array_list.Managed(SelectField).init(allocator);
    errdefer {
        for (list.items) |f| f.deinit(allocator);
        list.deinit();
    }
    while (true) {
        const f = try parseSelectItem(allocator, tok, cur, &q.has_count, false);
        list.append(f) catch {
            f.deinit(allocator);
            return error.OutOfMemory;
        };
        if (cur.tag != .comma) break;
        cur.* = try tok.next();
    }
    // SELECT * ADD expr AS col / SELECT * REMOVE col1, col2
    while (cur.tag == .kw_add or cur.tag == .kw_remove) {
        const is_add = cur.tag == .kw_add;
        cur.* = try tok.next();
        if (is_add) {
            const f = try parseSelectItem(allocator, tok, cur, &q.has_count, true);
            list.append(f) catch {
                f.deinit(allocator);
                return error.OutOfMemory;
            };
            continue;
        }
        while (true) {
            const rem_key = try parseGlobKey(allocator, tok, cur);
            list.append(.{ .key = rem_key, .alias = null, .is_remove = true }) catch {
                allocator.free(rem_key);
                return error.OutOfMemory;
            };
            if (cur.tag != .comma) break;
            cur.* = try tok.next();
        }
    }
    q.fields = try list.toOwnedSlice();
}

/// One SELECT item (or the operand of `* add`, when `is_add`).
fn parseSelectItem(
    allocator: std.mem.Allocator,
    tok: *Tokenizer,
    cur: *Token,
    has_count: *bool,
    is_add: bool,
) ParseError!SelectField {
    // count / count() / count(*): rows; count(expr): non-null values of expr
    if (cur.tag == .kw_count and !is_add) {
        cur.* = try tok.next();
        var arg: ?AggArg = null;
        if (cur.tag == .lparen) {
            cur.* = try tok.next();
            if (cur.tag == .star or cur.tag == .rparen) {
                if (cur.tag == .star) cur.* = try tok.next();
                if (cur.tag != .rparen) return error.UnexpectedToken;
                cur.* = try tok.next();
            } else {
                arg = try parseAggArg(allocator, tok, cur);
            }
        }
        if (arg) |a| {
            errdefer {
                allocator.free(a.key);
                if (a.expr) |e| destroyExpr(allocator, e);
            }
            const alias = try parseOptAlias(allocator, tok, cur) orelse try allocator.dupe(u8, "count");
            return .{ .key = a.key, .alias = alias, .agg = .count, .expr = a.expr };
        }
        has_count.* = true;
        const alias = try parseOptAlias(allocator, tok, cur);
        errdefer if (alias) |a| allocator.free(a);
        return .{ .key = try allocator.dupe(u8, "count"), .alias = alias, .agg = .count_rows };
    }

    // sum(..), avg(..), min(..), max(..), stddev(..), variance(..)
    if (aggFuncOf(cur.tag)) |af| agg: {
        if (is_add) break :agg;
        var peek_tok = tok.*;
        if ((try peek_tok.next()).tag == .lparen) {
            cur.* = try tok.next(); // the '('
            cur.* = try tok.next();
            const a = try parseAggArg(allocator, tok, cur);
            errdefer {
                allocator.free(a.key);
                if (a.expr) |e| destroyExpr(allocator, e);
            }
            // default output name: "sum", "avg", ... (lowercase, whatever the query's case)
            const alias = try parseOptAlias(allocator, tok, cur) orelse try allocator.dupe(u8, @tagName(af));
            return .{ .key = a.key, .alias = alias, .agg = af, .expr = a.expr };
        }
    }

    // EXPAND(field) / EXPAND(split(field, "delim")) — unnest into rows
    if (cur.tag == .kw_expand and !is_add) {
        cur.* = try tok.next();
        if (cur.tag != .lparen) return error.UnexpectedToken;
        cur.* = try tok.next();
        const in_split = cur.tag == .kw_split;
        if (in_split) {
            cur.* = try tok.next();
            if (cur.tag != .lparen) return error.UnexpectedToken;
            cur.* = try tok.next();
        }
        const exp_field = try parseGlobKey(allocator, tok, cur);
        errdefer allocator.free(exp_field);
        var delim: ?[]const u8 = null;
        errdefer if (delim) |d| allocator.free(d);
        if (in_split) {
            if (cur.tag != .comma) return error.UnexpectedToken;
            cur.* = try tok.next();
            if (cur.tag != .string) return error.UnexpectedToken;
            delim = try allocator.dupe(u8, cur.text);
            cur.* = try tok.next();
            if (cur.tag != .rparen) return error.UnexpectedToken; // close split(
            cur.* = try tok.next();
        }
        if (cur.tag != .rparen) return error.UnexpectedToken; // close expand(
        cur.* = try tok.next();
        const alias = try parseOptAlias(allocator, tok, cur);
        return .{ .key = exp_field, .alias = alias, .is_expand = true, .expand_split_delim = delim };
    }

    // Computed column starting with CASE or a literal: alias required.
    if (cur.tag == .kw_case or cur.tag == .string or cur.tag == .lparen) {
        const e = try parseExpr(allocator, tok, cur);
        errdefer destroyExpr(allocator, e);
        return computedField(allocator, tok, cur, e, is_add);
    }

    // A name: plain field (keyword names allowed), isnull(..), a function
    // call, or the start of an arithmetic expression.
    const name_tok = cur.*;
    const key = try parseGlobKey(allocator, tok, cur);
    if (cur.tag == .lparen) {
        if (std.ascii.eqlIgnoreCase(key, "isnull")) {
            allocator.free(key);
            return parseIsnull(allocator, tok, cur);
        }
        allocator.free(key);
        const fn_name = isFuncKeyword(name_tok.tag) orelse try lookupFunction(tok, name_tok);
        const call = try parseCall(allocator, tok, cur, fn_name);
        const e = try parseExprFrom(allocator, tok, cur, call);
        errdefer destroyExpr(allocator, e);
        return computedField(allocator, tok, cur, e, is_add);
    }
    if (isArithOp(cur.*)) {
        const field_node = allocator.create(Expr) catch {
            allocator.free(key);
            return error.OutOfMemory;
        };
        field_node.* = .{ .field = key }; // key ownership transferred into Expr
        const e = try parseExprFrom(allocator, tok, cur, field_node);
        errdefer destroyExpr(allocator, e);
        return computedField(allocator, tok, cur, e, is_add);
    }
    errdefer allocator.free(key);
    const alias = try parseOptAlias(allocator, tok, cur);
    return .{ .key = key, .alias = alias, .is_add = is_add };
}

/// A computed column `expr AS alias` (the alias is required).
fn computedField(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token, e: *Expr, is_add: bool) ParseError!SelectField {
    const alias = try parseReqAlias(allocator, tok, cur);
    errdefer allocator.free(alias);
    const key = try allocator.dupe(u8, alias);
    return .{ .key = key, .alias = alias, .expr = e, .is_add = is_add };
}

/// isnull(field, "default") [as alias] — `cur` is at the '('.
fn parseIsnull(allocator: std.mem.Allocator, tok: *Tokenizer, cur: *Token) ParseError!SelectField {
    cur.* = try tok.next();
    const key = try parseGlobKey(allocator, tok, cur);
    errdefer allocator.free(key);
    if (cur.tag != .comma) return error.UnexpectedToken;
    cur.* = try tok.next();
    if (cur.tag != .string) return error.UnexpectedToken;
    const def = try allocator.dupe(u8, cur.text);
    errdefer allocator.free(def);
    cur.* = try tok.next();
    if (cur.tag != .rparen) return error.UnexpectedToken;
    cur.* = try tok.next();
    const alias = try parseOptAlias(allocator, tok, cur);
    return .{ .key = key, .alias = alias, .default_val = def };
}

/// ORDER BY key [asc|desc], ... — keys may be aliases or 1-based ordinals.
fn parseOrderBy(
    allocator: std.mem.Allocator,
    tok: *Tokenizer,
    cur: *Token,
    fields: ?[]const SelectField,
) ParseError![]OrderField {
    cur.* = try tok.next(); // 'order'
    if (cur.tag != .kw_by) return error.UnexpectedToken;
    cur.* = try tok.next();
    var ob_list = std.array_list.Managed(OrderField).init(allocator);
    errdefer {
        for (ob_list.items) |f| allocator.free(f.key);
        ob_list.deinit();
    }
    while (true) {
        // Ordinal: ORDER BY 1, 2 — resolve to the nth SELECT field key/alias
        const key: []const u8 = if (cur.tag == .number) blk: {
            const ord = std.fmt.parseInt(usize, cur.text, 10) catch return error.UnexpectedToken;
            cur.* = try tok.next();
            if (fields != null and ord >= 1 and ord <= fields.?.len) {
                const sf = fields.?[ord - 1];
                break :blk try allocator.dupe(u8, if (sf.alias) |a| a else sf.key);
            }
            break :blk try std.fmt.allocPrint(allocator, "{d}", .{ord});
        } else try parseGlobKey(allocator, tok, cur);
        errdefer allocator.free(key);
        var dir: SortDir = .asc;
        if (cur.tag == .kw_asc) {
            cur.* = try tok.next();
        } else if (cur.tag == .kw_desc) {
            dir = .desc;
            cur.* = try tok.next();
        }
        try ob_list.append(.{ .key = key, .dir = dir });
        if (cur.tag != .comma) break;
        cur.* = try tok.next();
    }
    return ob_list.toOwnedSlice();
}

/// Parse a multi-query string separated by ';'. Each sub-query may have an INTO clause.
/// Returns a heap-owned slice of Query values.
pub fn parseMulti(allocator: std.mem.Allocator, src: []const u8) ParseError![]Query {
    var queries = std.array_list.Managed(Query).init(allocator);
    errdefer {
        for (queries.items) |q| q.deinit(allocator);
        queries.deinit();
    }

    // Split on ';' — but not inside quotes
    var parts = std.array_list.Managed([]const u8).init(allocator);
    defer parts.deinit();

    var start: usize = 0;
    var in_quote: ?u8 = null;
    for (src, 0..) |c, idx| {
        if (in_quote) |q| {
            if (c == q) in_quote = null;
        } else if (c == '\'' or c == '"') {
            in_quote = c;
        } else if (c == ';') {
            const part = std.mem.trim(u8, src[start..idx], " \t\n\r");
            if (part.len > 0) try parts.append(part);
            start = idx + 1;
        }
    }
    // Last segment
    const last = std.mem.trim(u8, src[start..], " \t\n\r");
    if (last.len > 0) try parts.append(last);

    for (parts.items) |part| {
        const q = try parse(allocator, part, null);
        try queries.append(q);
    }

    return try queries.toOwnedSlice();
}

// ---- GLOB MATCH ------------------------------------------------------------
// Segment-aware: * matches exactly one dot-path segment, ** matches rest.

pub fn globMatch(pattern: []const u8, key: []const u8) bool {
    return segMatch(pattern, key);
}

fn segMatch(pat: []const u8, key: []const u8) bool {
    if (pat.len == 0 and key.len == 0) return true;
    if (pat.len == 0) return false;

    const pd = std.mem.indexOfScalar(u8, pat, '.') orelse pat.len;
    const ps = pat[0..pd];
    const pr = if (pd < pat.len) pat[pd + 1 ..] else "";

    if (std.mem.eql(u8, ps, "**")) return true;

    if (key.len == 0) return false;
    const kd = std.mem.indexOfScalar(u8, key, '.') orelse key.len;
    const ks = key[0..kd];
    const kr = if (kd < key.len) key[kd + 1 ..] else "";

    if (!std.mem.eql(u8, ps, "*") and !std.mem.eql(u8, ps, ks)) return false;
    if (pr.len == 0 and kr.len == 0) return true;
    if (pr.len == 0 or kr.len == 0) return false;
    return segMatch(pr, kr);
}

// ---- RECORD MAP ------------------------------------------------------------

fn scopePrefixDepth(scope: []const u8) usize {
    var depth: usize = 0;
    var rest = scope;
    while (rest.len > 0) {
        depth += 1;
        const d = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
        if (d >= rest.len) break;
        rest = rest[d + 1 ..];
    }
    return depth;
}

fn keyPrefix(key: []const u8, depth: usize) ?[]const u8 {
    var rest = key;
    var consumed: usize = 0;
    var d: usize = 0;
    while (d < depth) : (d += 1) {
        const dot = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
        consumed += dot;
        if (d + 1 < depth) {
            if (dot >= rest.len) return null;
            consumed += 1;
            rest = rest[dot + 1 ..];
        }
    }
    return key[0..consumed];
}

fn keyRelative(key: []const u8, depth: usize) ?[]const u8 {
    var rest = key;
    var d: usize = 0;
    while (d < depth) : (d += 1) {
        const dot = std.mem.indexOfScalar(u8, rest, '.') orelse {
            return if (d + 1 == depth) "" else null;
        };
        rest = rest[dot + 1 ..];
    }
    return rest;
}

const Record = struct {
    prefix: []const u8,
    fields: std.StringHashMap(std.json.Value),

    fn deinit(self: *Record, allocator: std.mem.Allocator) void {
        allocator.free(self.prefix);
        self.fields.deinit();
    }
};

fn buildRecords(
    allocator: std.mem.Allocator,
    flat: std.json.ObjectMap,
    scope: []const u8,
) ![]Record {
    const depth = scopePrefixDepth(scope);
    var prefix_index = std.StringHashMap(usize).init(allocator);
    defer prefix_index.deinit();

    var records = std.array_list.Managed(Record).init(allocator);
    errdefer {
        for (records.items) |*r| r.deinit(allocator);
        records.deinit();
    }

    var it = flat.iterator();
    while (it.next()) |entry| {
        const flat_key = entry.key_ptr.*;
        const val = entry.value_ptr.*;

        const pfx = keyPrefix(flat_key, depth) orelse continue;
        const rel = keyRelative(flat_key, depth) orelse continue;
        if (rel.len == 0) continue;
        if (!globMatch(scope, pfx)) continue;

        const gop = try prefix_index.getOrPut(pfx);
        if (!gop.found_existing) {
            const owned_pfx = try allocator.dupe(u8, pfx);
            gop.value_ptr.* = records.items.len;
            try records.append(.{
                .prefix = owned_pfx,
                .fields = std.StringHashMap(std.json.Value).init(allocator),
            });
        }
        try records.items[gop.value_ptr.*].fields.put(rel, val);
    }

    return records.toOwnedSlice();
}

fn detectArrayRoots(
    allocator: std.mem.Allocator,
    flat: std.json.ObjectMap,
) ![][]const u8 {
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();
    var roots = std.array_list.Managed([]const u8).init(allocator);
    errdefer {
        for (roots.items) |r| allocator.free(r);
        roots.deinit();
    }

    var it = flat.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        var rest = key;
        var seg_start: usize = 0;
        while (rest.len > 0) {
            const dot = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
            const seg = rest[0..dot];
            _ = std.fmt.parseInt(usize, seg, 10) catch {
                seg_start += dot + 1;
                if (dot < rest.len) {
                    rest = rest[dot + 1 ..];
                } else {
                    rest = "";
                }
                continue;
            };
            // numeric segment found; the array root prefix ends just before it
            if (seg_start == 0) break; // top-level numeric, skip
            const root_prefix = key[0 .. seg_start - 1];
            const root_pat = try std.fmt.allocPrint(allocator, "{s}.*", .{root_prefix});
            if (seen.contains(root_pat)) {
                allocator.free(root_pat);
            } else {
                try seen.put(root_pat, {});
                try roots.append(root_pat);
            }
            break;
        }
    }
    return roots.toOwnedSlice();
}

// ---- VALUE EVALUATION ------------------------------------------------------

fn evalCond(json_val: std.json.Value, op: Op, q_val: Value) bool {
    return switch (op) {
        .eq => switch (q_val) {
            .string => |s| json_val == .string and std.mem.eql(u8, json_val.string, s),
            .number => |n| switch (json_val) {
                .integer => |i| @as(f64, @floatFromInt(i)) == n,
                .float => |f| f == n,
                else => false,
            },
            .boolean => |b| json_val == .bool and json_val.bool == b,
            .null_val => json_val == .null,
            .string_list => false,
        },
        .neq => !evalCond(json_val, .eq, q_val),
        .gt => cmpNum(json_val, q_val) > 0,
        .lt => cmpNum(json_val, q_val) < 0,
        .gte => cmpNum(json_val, q_val) >= 0,
        .lte => cmpNum(json_val, q_val) <= 0,
        .like => json_val == .string and q_val == .string and
            globLike(q_val.string, json_val.string),
        .contains => json_val == .string and q_val == .string and
            std.mem.indexOf(u8, json_val.string, q_val.string) != null,
        .not_contains => json_val == .string and q_val == .string and
            std.mem.indexOf(u8, json_val.string, q_val.string) == null,
        .starts_with => json_val == .string and q_val == .string and
            std.mem.startsWith(u8, json_val.string, q_val.string),
        .ends_with => json_val == .string and q_val == .string and
            std.mem.endsWith(u8, json_val.string, q_val.string),
        .in_list => blk: {
            if (q_val != .string_list) break :blk false;
            if (json_val == .string) {
                for (q_val.string_list) |s| {
                    if (std.mem.eql(u8, json_val.string, s)) break :blk true;
                }
            } else if (json_val == .integer) {
                for (q_val.string_list) |s| {
                    const n = std.fmt.parseFloat(f64, s) catch continue;
                    if (@as(f64, @floatFromInt(json_val.integer)) == n) break :blk true;
                }
            } else if (json_val == .float) {
                for (q_val.string_list) |s| {
                    const n = std.fmt.parseFloat(f64, s) catch continue;
                    if (json_val.float == n) break :blk true;
                }
            }
            break :blk false;
        },
        .not_in_list => !evalCond(json_val, .in_list, q_val),
        .is_null => json_val == .null,
        .is_not_null => json_val != .null,
        // has_key / not_has_key are handled before calling evalCond (key-existence check at object level)
        // If we reach here the key was found, so has_key=true, not_has_key=false.
        .has_key => true,
        .not_has_key => false,
        .matches => json_val == .string and q_val == .string and
            simpleRegexMatch(q_val.string, json_val.string),
        .not_matches => json_val == .string and q_val == .string and
            !simpleRegexMatch(q_val.string, json_val.string),
    };
}

fn cmpNum(jv: std.json.Value, qv: Value) i32 {
    const a: f64 = switch (jv) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => return -2,
    };
    const b: f64 = switch (qv) {
        .number => |n| n,
        else => return -2,
    };
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

// ---- SCOPED EXECUTION ------------------------------------------------------

fn recordFieldMatch(
    record: *const Record,
    cond_key: []const u8,
    op: Op,
    q_val: Value,
) bool {
    var it = record.fields.iterator();
    while (it.next()) |e| {
        if (globMatch(cond_key, e.key_ptr.*) and evalCond(e.value_ptr.*, op, q_val))
            return true;
    }
    return false;
}

fn recordPassesWhere(record: *const Record, w: WhereClause) bool {
    return switch (w) {
        .leaf => |c| recordFieldMatch(record, c.key, c.op, c.value),
        .and_ => |b| recordPassesWhere(record, b.lhs.*) and recordPassesWhere(record, b.rhs.*),
        .or_ => |b| recordPassesWhere(record, b.lhs.*) or recordPassesWhere(record, b.rhs.*),
        .not_ => |inner| !recordPassesWhere(record, inner.*),
    };
}

fn execScoped(
    allocator: std.mem.Allocator,
    flat: std.json.ObjectMap,
    scope: []const u8,
    fields: ?[]const SelectField,
    where: ?WhereClause,
) !std.json.ObjectMap {
    var out = std.json.ObjectMap{};
    errdefer {
        var oit = out.iterator();
        while (oit.next()) |e| allocator.free(e.key_ptr.*);
        out.deinit(allocator);
    }

    const records = try buildRecords(allocator, flat, scope);
    defer {
        for (records) |*r| {
            var rc: Record = r.*;
            rc.deinit(allocator);
        }
        allocator.free(records);
    }

    for (records) |*record| {
        if (where) |w| {
            if (!recordPassesWhere(record, w)) continue;
        }

        var fit = record.fields.iterator();
        while (fit.next()) |fe| {
            const rel_key = fe.key_ptr.*;
            const val = fe.value_ptr.*;

            if (fields) |sel| {
                var matched = false;
                var out_rel = rel_key;
                for (sel) |f| {
                    if (globMatch(f.key, rel_key)) {
                        matched = true;
                        if (f.alias) |a| out_rel = a;
                        break;
                    }
                }
                if (!matched) continue;
                const full_key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ record.prefix, out_rel });
                try out.put(allocator, full_key, val);
            } else {
                const full_key = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ record.prefix, rel_key });
                try out.put(allocator, full_key, val);
            }
        }
    }
    return out;
}

// ---- FLAT EXECUTION --------------------------------------------------------

/// Evaluate a WhereClause against a single flat key/value pair.
/// In flat mode, each key is tested individually against condition keys via globMatch.
fn flatRecordPassesWhere(k: []const u8, v: std.json.Value, w: WhereClause) bool {
    return switch (w) {
        .leaf => |c| globMatch(c.key, k) and evalCond(v, c.op, c.value),
        .and_ => |b| flatRecordPassesWhere(k, v, b.lhs.*) and flatRecordPassesWhere(k, v, b.rhs.*),
        .or_ => |b| flatRecordPassesWhere(k, v, b.lhs.*) or flatRecordPassesWhere(k, v, b.rhs.*),
        .not_ => |inner| !flatRecordPassesWhere(k, v, inner.*),
    };
}

fn execFlat(
    allocator: std.mem.Allocator,
    flat: std.json.ObjectMap,
    fields: ?[]const SelectField,
    where: ?WhereClause,
) !std.json.ObjectMap {
    var out = std.json.ObjectMap{};
    errdefer {
        var oit = out.iterator();
        while (oit.next()) |e| allocator.free(e.key_ptr.*);
        out.deinit(allocator);
    }

    var it = flat.iterator();
    while (it.next()) |entry| {
        const k = entry.key_ptr.*;
        const v = entry.value_ptr.*;

        if (where) |w| {
            if (!flatRecordPassesWhere(k, v, w)) continue;
        }

        if (fields) |sel| {
            var matched = false;
            var out_key: []const u8 = k;
            for (sel) |f| {
                if (globMatch(f.key, k)) {
                    matched = true;
                    if (f.alias) |a| out_key = a;
                    break;
                }
            }
            if (!matched) continue;
            try out.put(allocator, try allocator.dupe(u8, out_key), v);
        } else {
            try out.put(allocator, try allocator.dupe(u8, k), v);
        }
    }
    return out;
}

// ---- NATIVE (NON-FLAT) QUERY EXECUTION ------------------------------------
//
// Works directly on the parsed std.json.Value tree.
// Scoped mode  [arr.*]  → returns Value{ .array } of projected objects.
// [*] auto mode         → returns Value{ .object } keyed by array name.
// No-scope              → not supported here; caller should just pass Value through.

/// Navigate a dot-path like "users" or "data.records" into the Value tree.
/// Returns null if any segment is missing or the path is empty.
fn navigatePath(root: std.json.Value, path: []const u8) ?std.json.Value {
    if (path.len == 0) return root;
    var cur = root;
    var rest = path;
    while (rest.len > 0) {
        const dot = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
        const seg = rest[0..dot];
        switch (cur) {
            .object => |obj| {
                cur = obj.get(seg) orelse return null;
            },
            .array => |arr| {
                const idx = std.fmt.parseInt(usize, seg, 10) catch return null;
                if (idx >= arr.items.len) return null;
                cur = arr.items[idx];
            },
            else => return null,
        }
        rest = if (dot < rest.len) rest[dot + 1 ..] else "";
    }
    return cur;
}

/// Evaluate a WHERE condition against a json.Value object (one record).
fn nativeRecordCond(obj: std.json.ObjectMap, cond_key: []const u8, op: Op, q_val: Value) bool {
    // cond_key may be a simple field name or a dot-path; navigate into the object
    // We also support glob match against the flat-projected field names within a record.
    // Simple case: direct key lookup first.
    if (std.mem.indexOfScalar(u8, cond_key, '.') == null and
        std.mem.indexOfScalar(u8, cond_key, '*') == null)
    {
        const jv = obj.get(cond_key) orelse return false;
        return evalCond(jv, op, q_val);
    }
    // Dot-path navigation
    const v = navigatePath(.{ .object = obj }, cond_key) orelse return false;
    return evalCond(v, op, q_val);
}

fn nativeRecordPassesWhere(obj: std.json.ObjectMap, w: WhereClause) bool {
    return switch (w) {
        .leaf => |c| nativeRecordCond(obj, c.key, c.op, c.value),
        .and_ => |b| nativeRecordPassesWhere(obj, b.lhs.*) and nativeRecordPassesWhere(obj, b.rhs.*),
        .or_ => |b| nativeRecordPassesWhere(obj, b.lhs.*) or nativeRecordPassesWhere(obj, b.rhs.*),
        .not_ => |inner| !nativeRecordPassesWhere(obj, inner.*),
    };
}

/// Project one record object through a SELECT field list.
/// Returns a new ObjectMap (caller owns it — but values are borrowed from src).
fn nativeProjectRecord(
    allocator: std.mem.Allocator,
    obj: std.json.ObjectMap,
    fields: []const SelectField,
) !std.json.ObjectMap {
    var out = std.json.ObjectMap{};
    errdefer {
        var it = out.iterator();
        while (it.next()) |e| allocator.free(e.key_ptr.*);
        out.deinit(allocator);
    }
    for (fields) |f| {
        // f.key is a relative field name (possibly with dots for nested)
        if (std.mem.indexOfScalar(u8, f.key, '*') != null) {
            // glob — iterate object keys and match
            var it = obj.iterator();
            while (it.next()) |e| {
                if (globMatch(f.key, e.key_ptr.*)) {
                    const out_key = if (f.alias) |a|
                        try allocator.dupe(u8, a)
                    else
                        try allocator.dupe(u8, e.key_ptr.*);
                    try out.put(allocator, out_key, e.value_ptr.*);
                }
            }
        } else {
            const jv = navigatePath(.{ .object = obj }, f.key) orelse continue;
            const out_key = try allocator.dupe(u8, if (f.alias) |a| a else f.key);
            try out.put(allocator, out_key, jv);
        }
    }
    return out;
}

/// Execute a scoped native query on a single array Value.
/// Returns Value{ .array } of result objects.
/// Get a numeric value from a json.Value for comparison purposes.
fn sortKeyNum(v: std.json.Value) f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => 0,
    };
}

/// Compare two projected-record Values by an ordered list of OrderFields.
/// Records are std.json.Value{ .object = ... }.
fn compareRecords(order_by: []const OrderField, a: std.json.Value, b: std.json.Value) std.math.Order {
    const ao = switch (a) {
        .object => |o| o,
        else => return .eq,
    };
    const bo = switch (b) {
        .object => |o| o,
        else => return .eq,
    };
    for (order_by) |f| {
        const av = navigatePath(.{ .object = ao }, f.key) orelse .null;
        const bv = navigatePath(.{ .object = bo }, f.key) orelse .null;
        const ord: std.math.Order = blk: {
            // Both strings
            if (av == .string and bv == .string) {
                const cmp = std.mem.order(u8, av.string, bv.string);
                break :blk cmp;
            }
            // Numeric comparison (integer or float)
            const an = sortKeyNum(av);
            const bn = sortKeyNum(bv);
            if (an < bn) break :blk .lt;
            if (an > bn) break :blk .gt;
            break :blk .eq;
        };
        if (ord == .eq) continue;
        return if (f.dir == .asc) ord else switch (ord) {
            .lt => .gt,
            .gt => .lt,
            .eq => .eq,
        };
    }
    return .eq;
}

fn nativeExecOnArray(
    allocator: std.mem.Allocator,
    arr: std.json.Array,
    fields: ?[]const SelectField,
    where: ?WhereClause,
    order_by: ?[]const OrderField,
    limit: ?usize,
) !std.json.Value {
    var out = std.json.Array.init(allocator);
    errdefer {
        for (out.items) |item| freeValue(allocator, item);
        out.deinit();
    }

    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };

        if (where) |w| {
            if (!nativeRecordPassesWhere(obj, w)) continue;
        }

        const record: std.json.Value = if (fields) |sel| blk: {
            const projected = try nativeProjectRecord(allocator, obj, sel);
            break :blk .{ .object = projected };
        } else blk: {
            var copy = std.json.ObjectMap{};
            errdefer {
                var it = copy.iterator();
                while (it.next()) |e| allocator.free(e.key_ptr.*);
                copy.deinit(allocator);
            }
            var it = obj.iterator();
            while (it.next()) |e| {
                try copy.put(allocator, try allocator.dupe(u8, e.key_ptr.*), e.value_ptr.*);
            }
            break :blk .{ .object = copy };
        };

        try out.append(record);
    }

    // Sort if requested (stable insertion-sort for small N, good enough)
    if (order_by) |ob| {
        // std.sort on the slice
        const Ctx = struct {
            ob: []const OrderField,
            pub fn lessThan(ctx: @This(), a: std.json.Value, b: std.json.Value) bool {
                return compareRecords(ctx.ob, a, b) == .lt;
            }
        };
        std.sort.block(std.json.Value, out.items, Ctx{ .ob = ob }, Ctx.lessThan);
    }

    // Apply limit AFTER sort
    if (limit) |lim| {
        if (out.items.len > lim) {
            // Free the tail items we're discarding
            for (out.items[lim..]) |item| freeValue(allocator, item);
            out.shrinkRetainingCapacity(lim);
        }
    }

    return .{ .array = out };
}

/// Entry point for native (non-flat) query execution.
/// Returns a newly allocated std.json.Value that the caller must deinit.
/// For scoped queries the returned Value is an array (or object of arrays for [*]).
/// For no-scope queries the original value is returned unchanged (no allocation).
pub fn execQueryNative(
    allocator: std.mem.Allocator,
    root: std.json.Value,
    query: Query,
) !std.json.Value {
    const sel = if (query.fields) |f| @as(?[]const SelectField, f) else null;

    if (query.scope_pattern) |scope| {
        if (std.mem.eql(u8, scope, "*")) {
            // Auto mode: if root is a bare array, query it directly.
            // If root is an object, find all top-level array fields and query each.
            const ob = if (query.order_by) |o| @as(?[]const OrderField, o) else null;
            switch (root) {
                .array => |arr| {
                    // Input is a top-level JSON array: [{ ... }, { ... }]
                    return nativeExecOnArray(allocator, arr, sel, query.where, ob, query.limit);
                },
                .object => |top_obj| {
                    var result_obj = std.json.ObjectMap{};
                    errdefer {
                        var rit = result_obj.iterator();
                        while (rit.next()) |e| {
                            allocator.free(e.key_ptr.*);
                            freeValue(allocator, e.value_ptr.*);
                        }
                        result_obj.deinit(allocator);
                    }
                    var it = top_obj.iterator();
                    while (it.next()) |e| {
                        switch (e.value_ptr.*) {
                            .array => |arr| {
                                const arr_result = try nativeExecOnArray(allocator, arr, sel, query.where, ob, query.limit);
                                const owned_key = try allocator.dupe(u8, e.key_ptr.*);
                                try result_obj.put(allocator, owned_key, arr_result);
                            },
                            else => {},
                        }
                    }
                    return .{ .object = result_obj };
                },
                else => return .{ .array = std.json.Array.init(allocator) },
            }
        } else {
            // Scoped: scope is like "users.*" — prefix before ".*" is the path
            const star_pos = std.mem.lastIndexOfScalar(u8, scope, '*') orelse scope.len;
            // strip trailing ".*" to get the navigation path
            const nav_path = if (star_pos >= 2 and scope[star_pos - 1] == '.')
                scope[0 .. star_pos - 1]
            else
                scope;

            const ob = if (query.order_by) |o| @as(?[]const OrderField, o) else null;
            const target = navigatePath(root, nav_path) orelse return .{ .array = std.json.Array.init(allocator) };
            switch (target) {
                .array => |arr| return nativeExecOnArray(allocator, arr, sel, query.where, ob, query.limit),
                else => return .{ .array = std.json.Array.init(allocator) },
            }
        }
    }

    // No scope: return root unchanged (caller handles pass-through)
    return root;
}

/// Recursively free a Value that was allocated by nativeExecOnArray / execQueryNative.
/// OWNERSHIP MODEL: Only the top-level container structure is owned:
///   - Array items are freed (they are projected ObjectMap copies)
///   - ObjectMap keys are freed (they were dupe'd)
///   - ObjectMap values are NOT recursed into — they are borrowed from parsed.value
///   - The exception is the [*] case which returns an ObjectMap of Arrays — those
///     are recursed one level to free the inner Arrays and their items.
pub fn freeValue(allocator: std.mem.Allocator, val: std.json.Value) void {
    switch (val) {
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |e| {
                allocator.free(e.key_ptr.*);
                // Values inside projected records are borrowed from parsed.value —
                // only recurse if the value is itself an array (the [*] case)
                switch (e.value_ptr.*) {
                    .array => freeValue(allocator, e.value_ptr.*),
                    else => {}, // borrowed — do NOT free
                }
            }
            var mut = obj;
            mut.deinit(allocator);
        },
        .array => |arr| {
            for (arr.items) |item| freeValue(allocator, item);
            var mut = arr;
            mut.deinit();
        },
        else => {},
    }
}

// ---- PUBLIC ENTRY POINT ----------------------------------------------------

pub fn execQuery(
    allocator: std.mem.Allocator,
    flat: std.json.ObjectMap,
    query: Query,
) !std.json.ObjectMap {
    const sel = if (query.fields) |f| @as(?[]const SelectField, f) else null;

    if (query.scope_pattern) |scope| {
        if (std.mem.eql(u8, scope, "*")) {
            const roots = try detectArrayRoots(allocator, flat);
            defer {
                for (roots) |r| allocator.free(r);
                allocator.free(roots);
            }
            var merged = std.json.ObjectMap{};
            errdefer {
                var mit = merged.iterator();
                while (mit.next()) |e| allocator.free(e.key_ptr.*);
                merged.deinit(allocator);
            }
            for (roots) |root| {
                var part = try execScoped(allocator, flat, root, sel, query.where);
                defer {
                    var pit = part.iterator();
                    while (pit.next()) |e| allocator.free(e.key_ptr.*);
                    part.deinit(allocator);
                }
                var pit = part.iterator();
                while (pit.next()) |e| {
                    const owned = try allocator.dupe(u8, e.key_ptr.*);
                    try merged.put(allocator, owned, e.value_ptr.*);
                }
            }
            return merged;
        } else {
            return execScoped(allocator, flat, scope, sel, query.where);
        }
    } else {
        return execFlat(allocator, flat, sel, query.where);
    }
}
