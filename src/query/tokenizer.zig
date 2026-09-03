//! tokenizer.zig — SQL query tokenizer (lexer).
//!
//! Converts a query string into a stream of tokens for the parser.
//! Handles keywords, identifiers, string/number literals, and operators.

const std = @import("std");

pub const TokTag = enum {
    kw_select,
    kw_where,
    kw_as,
    kw_and,
    kw_or,
    kw_like,
    kw_true,
    kw_false,
    kw_null,
    kw_order,
    kw_by,
    kw_asc,
    kw_desc,
    kw_limit,
    kw_group,
    kw_count,
    kw_contains,
    kw_starts_with,
    kw_ends_with,
    kw_not,
    kw_in,
    kw_is,
    kw_isnull,
    // function names
    kw_upper,
    kw_lower,
    kw_len,
    kw_round,
    kw_floor,
    kw_ceil,
    kw_trim,
    kw_concat,
    kw_substr,
    kw_abs,
    kw_to_str,
    // aggregate functions
    kw_sum,
    kw_avg,
    kw_min,
    kw_max,
    kw_stddev,
    kw_variance,
    // new function keywords
    kw_to_number,
    kw_format,
    kw_coalesce,
    kw_replace,
    kw_lpad,
    kw_rpad,
    kw_split,
    // CASE WHEN THEN ELSE END
    kw_case,
    kw_when,
    kw_then,
    kw_else,
    kw_end,
    // has() operator
    kw_has,
    kw_matches,
    kw_type,
    kw_keys,
    kw_values,
    kw_to_entries,
    // query modifiers
    kw_distinct,
    kw_expand,
    kw_add,
    kw_remove,
    kw_into,
    kw_between,
    kw_having,
    // date/time function keywords
    kw_now,
    kw_now_epoch,
    kw_now_ms,
    kw_from_epoch,
    kw_from_epoch_ms,
    kw_to_epoch,
    kw_to_epoch_ms,
    kw_date_part,
    kw_epoch_min,
    kw_epoch_hour,
    kw_epoch_day,
    kw_epoch_week,
    // arithmetic operators
    op_plus,
    op_minus,
    op_div,
    op_mod,
    op_alt, // // alternative (coalesce) operator — field // default
    lparen,
    rparen,
    op_eq,
    op_neq,
    op_gt,
    op_lt,
    op_gte,
    op_lte,
    ident,
    string,
    number,
    comma,
    dot,
    star,
    lbracket,
    rbracket,
    eof,
};

pub const Token = struct { tag: TokTag, text: []const u8 };

pub const Tokenizer = struct {
    src: []const u8,
    pos: usize = 0,

    pub fn peek(self: *Tokenizer) ?u8 {
        return if (self.pos < self.src.len) self.src[self.pos] else null;
    }
    pub fn advance(self: *Tokenizer) void {
        self.pos += 1;
    }
    pub fn skipWs(self: *Tokenizer) void {
        while (self.peek()) |c| {
            if (c == ' ' or c == '\t' or c == '\n' or c == '\r') self.advance() else break;
        }
    }

    pub fn next(self: *Tokenizer) !Token {
        self.skipWs();
        if (self.pos >= self.src.len) return .{ .tag = .eof, .text = "" };
        const start = self.pos;
        const c = self.src[self.pos];

        if (c == '"' or c == '\'') {
            const quote = c;
            self.advance();
            const s = self.pos;
            while (self.peek()) |ch| {
                if (ch == quote) break;
                self.advance();
            }
            const text = self.src[s..self.pos];
            if (self.peek() != null) self.advance();
            return .{ .tag = .string, .text = text };
        }
        if ((c >= '0' and c <= '9') or
            (c == '-' and self.pos + 1 < self.src.len and
                self.src[self.pos + 1] >= '0' and self.src[self.pos + 1] <= '9'))
        {
            self.advance();
            while (self.peek()) |ch| {
                if ((ch >= '0' and ch <= '9') or ch == '.') self.advance() else break;
            }
            return .{ .tag = .number, .text = self.src[start..self.pos] };
        }
        if (c == '!' and self.pos + 1 < self.src.len and self.src[self.pos + 1] == '=') {
            self.pos += 2;
            return .{ .tag = .op_neq, .text = self.src[start..self.pos] };
        }
        if (c == '>' and self.pos + 1 < self.src.len and self.src[self.pos + 1] == '=') {
            self.pos += 2;
            return .{ .tag = .op_gte, .text = self.src[start..self.pos] };
        }
        if (c == '<' and self.pos + 1 < self.src.len and self.src[self.pos + 1] == '=') {
            self.pos += 2;
            return .{ .tag = .op_lte, .text = self.src[start..self.pos] };
        }
        switch (c) {
            '=' => {
                self.advance();
                return .{ .tag = .op_eq, .text = self.src[start..self.pos] };
            },
            '>' => {
                self.advance();
                return .{ .tag = .op_gt, .text = self.src[start..self.pos] };
            },
            '<' => {
                self.advance();
                return .{ .tag = .op_lt, .text = self.src[start..self.pos] };
            },
            ',' => {
                self.advance();
                return .{ .tag = .comma, .text = self.src[start..self.pos] };
            },
            '.' => {
                self.advance();
                return .{ .tag = .dot, .text = self.src[start..self.pos] };
            },
            '*' => {
                self.advance();
                return .{ .tag = .star, .text = self.src[start..self.pos] };
            },
            '[' => {
                self.advance();
                return .{ .tag = .lbracket, .text = self.src[start..self.pos] };
            },
            ']' => {
                self.advance();
                return .{ .tag = .rbracket, .text = self.src[start..self.pos] };
            },
            '(' => {
                self.advance();
                return .{ .tag = .lparen, .text = self.src[start..self.pos] };
            },
            ')' => {
                self.advance();
                return .{ .tag = .rparen, .text = self.src[start..self.pos] };
            },
            '+' => {
                self.advance();
                return .{ .tag = .op_plus, .text = self.src[start..self.pos] };
            },
            '-' => {
                // standalone minus (not before a digit — that's a negative number literal)
                self.advance();
                return .{ .tag = .op_minus, .text = self.src[start..self.pos] };
            },
            '/' => {
                // Check for // (alternative/coalesce operator) before treating as division
                if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '/') {
                    self.pos += 2;
                    return .{ .tag = .op_alt, .text = self.src[start..self.pos] };
                }
                self.advance();
                return .{ .tag = .op_div, .text = self.src[start..self.pos] };
            },
            '%' => {
                self.advance();
                return .{ .tag = .op_mod, .text = self.src[start..self.pos] };
            },
            else => {},
        }
        if (isIdentStart(c)) {
            while (self.peek()) |ch| {
                if (isIdentCont(ch)) self.advance() else break;
            }
            const word = self.src[start..self.pos];
            return .{ .tag = keywordTag(word) orelse .ident, .text = word };
        }
        return error.UnexpectedChar;
    }
};

pub fn isIdentStart(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_' or c == '-';
}
pub fn isIdentCont(c: u8) bool {
    return isIdentStart(c) or (c >= '0' and c <= '9');
}
pub fn keywordTag(w: []const u8) ?TokTag {
    const kws = .{
        .{ "select", TokTag.kw_select },           .{ "where", TokTag.kw_where },
        .{ "as", TokTag.kw_as },                   .{ "and", TokTag.kw_and },
        .{ "or", TokTag.kw_or },                   .{ "like", TokTag.kw_like },
        .{ "true", TokTag.kw_true },               .{ "false", TokTag.kw_false },
        .{ "null", TokTag.kw_null },               .{ "order", TokTag.kw_order },
        .{ "by", TokTag.kw_by },                   .{ "asc", TokTag.kw_asc },
        .{ "desc", TokTag.kw_desc },               .{ "limit", TokTag.kw_limit },
        .{ "group", TokTag.kw_group },             .{ "count", TokTag.kw_count },
        .{ "contains", TokTag.kw_contains },       .{ "not", TokTag.kw_not },
        .{ "starts_with", TokTag.kw_starts_with }, .{ "ends_with", TokTag.kw_ends_with },
        .{ "in", TokTag.kw_in },                   .{ "is", TokTag.kw_is },
        .{ "isnull", TokTag.kw_isnull },           .{ "upper", TokTag.kw_upper },
        .{ "lower", TokTag.kw_lower },             .{ "len", TokTag.kw_len },
        .{ "round", TokTag.kw_round },             .{ "floor", TokTag.kw_floor },
        .{ "ceil", TokTag.kw_ceil },               .{ "trim", TokTag.kw_trim },
        .{ "concat", TokTag.kw_concat },           .{ "substr", TokTag.kw_substr },
        .{ "abs", TokTag.kw_abs },                 .{ "to_str", TokTag.kw_to_str },
        .{ "sum", TokTag.kw_sum },                 .{ "avg", TokTag.kw_avg },
        .{ "min", TokTag.kw_min },                 .{ "max", TokTag.kw_max },
        .{ "stddev", TokTag.kw_stddev },           .{ "variance", TokTag.kw_variance },
        .{ "to_number", TokTag.kw_to_number },     .{ "format", TokTag.kw_format },
        .{ "coalesce", TokTag.kw_coalesce },       .{ "replace", TokTag.kw_replace },
        .{ "lpad", TokTag.kw_lpad },               .{ "rpad", TokTag.kw_rpad },
        .{ "split", TokTag.kw_split },             .{ "type", TokTag.kw_type },
        .{ "keys", TokTag.kw_keys },               .{ "values", TokTag.kw_values },
        .{ "to_entries", TokTag.kw_to_entries },   .{ "case", TokTag.kw_case },
        .{ "when", TokTag.kw_when },               .{ "then", TokTag.kw_then },
        .{ "else", TokTag.kw_else },               .{ "end", TokTag.kw_end },
        .{ "has", TokTag.kw_has },                 .{ "matches", TokTag.kw_matches },
        .{ "distinct", TokTag.kw_distinct },       .{ "expand", TokTag.kw_expand },
        .{ "add", TokTag.kw_add },                 .{ "remove", TokTag.kw_remove },
        .{ "into", TokTag.kw_into },               .{ "between", TokTag.kw_between },
        .{ "having", TokTag.kw_having },           .{ "now", TokTag.kw_now },
        .{ "now_epoch", TokTag.kw_now_epoch },     .{ "now_ms", TokTag.kw_now_ms },
        .{ "from_epoch", TokTag.kw_from_epoch },   .{ "from_epoch_ms", TokTag.kw_from_epoch_ms },
        .{ "to_epoch", TokTag.kw_to_epoch },       .{ "to_epoch_ms", TokTag.kw_to_epoch_ms },
        .{ "date_part", TokTag.kw_date_part },     .{ "epoch_min", TokTag.kw_epoch_min },
        .{ "epoch_hour", TokTag.kw_epoch_hour },   .{ "epoch_day", TokTag.kw_epoch_day },
        .{ "epoch_week", TokTag.kw_epoch_week },
    };
    inline for (kws) |kw| {
        if (std.mem.eql(u8, w, kw[0])) return kw[1];
    }
    return null;
}

/// Returns true if a token tag can serve as a field name or field-name segment.
/// This includes plain identifiers, numbers, and all keyword tags (since any
/// keyword can also be used as a field name, e.g. `count`, `level`, `in`, `is`).
///
/// This replaces the two duplicated ~60-entry keyword lists that were inlined
/// in parseGlobKey's isIdentLike / nextIsIdentLike checks.
pub fn isKeywordIdent(tag: TokTag) bool {
    return switch (tag) {
        .ident, .number => true,
        // SQL clauses
        .kw_select, .kw_where, .kw_as, .kw_and, .kw_or, .kw_like,
        .kw_true, .kw_false, .kw_null, .kw_order, .kw_by, .kw_asc,
        .kw_desc, .kw_limit, .kw_group, .kw_count, .kw_contains,
        .kw_not, .kw_in, .kw_is, .kw_starts_with, .kw_ends_with,
        .kw_isnull, .kw_between, .kw_having,
        // String functions
        .kw_upper, .kw_lower, .kw_len, .kw_round, .kw_floor, .kw_ceil,
        .kw_trim, .kw_concat, .kw_substr, .kw_abs, .kw_to_str,
        .kw_to_number, .kw_format, .kw_coalesce, .kw_replace,
        .kw_lpad, .kw_rpad, .kw_split, .kw_type,
        // Structural functions
        .kw_keys, .kw_values, .kw_to_entries,
        // Aggregate functions
        .kw_sum, .kw_avg, .kw_min, .kw_max, .kw_stddev, .kw_variance,
        // CASE expression
        .kw_case, .kw_when, .kw_then, .kw_else, .kw_end,
        // Operators
        .kw_has, .kw_matches,
        // Query modifiers
        .kw_distinct, .kw_expand, .kw_add, .kw_remove, .kw_into,
        // Date/time functions
        .kw_now, .kw_now_epoch, .kw_now_ms, .kw_from_epoch,
        .kw_from_epoch_ms, .kw_to_epoch, .kw_to_epoch_ms, .kw_date_part,
        .kw_epoch_min, .kw_epoch_hour, .kw_epoch_day, .kw_epoch_week,
        => true,
        else => false,
    };
}

