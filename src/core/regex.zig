//! regex.zig — Shared regex and glob-like matching engine.
//!
//! Used by both query.zig (buffered executor) and stream_exec.zig (streaming
//! executor) for the `matches` / `not matches` and `like` operators.
//!
//! Supports: . (any char), * (0+ of prev), + (1+ of prev), ? (0 or 1 of prev),
//!           ^ (start anchor), $ (end anchor), \d \w \s \D \W \S, [abc], [^abc],
//!           [a-z] ranges, literal escape \.  \*  etc., | alternation.
//!
//! This is an NFA-style backtracking matcher — sufficient for query filtering.

const std = @import("std");

/// SQL LIKE pattern matching with % (any sequence) and _ (single char).
pub fn globLike(pattern: []const u8, str: []const u8) bool {
    var pi: usize = 0;
    var si: usize = 0;
    var sp: usize = std.math.maxInt(usize);
    var ss: usize = 0;
    while (si < str.len) {
        if (pi < pattern.len and (pattern[pi] == '_' or pattern[pi] == str[si])) {
            pi += 1;
            si += 1;
        } else if (pi < pattern.len and pattern[pi] == '%') {
            sp = pi;
            ss = si;
            pi += 1;
        } else if (sp != std.math.maxInt(usize)) {
            ss += 1;
            si = ss;
            pi = sp + 1;
        } else return false;
    }
    while (pi < pattern.len and pattern[pi] == '%') pi += 1;
    return pi == pattern.len;
}

/// Top-level match entry point: handles `A|B|C` alternation by trying each
/// branch in turn.  Branches are split on unescaped `|` outside `[…]`.
pub fn simpleRegexMatch(pattern: []const u8, str: []const u8) bool {
    var seg_start: usize = 0;
    var in_class: bool = false;
    var i: usize = 0;
    while (i < pattern.len) : (i += 1) {
        const c = pattern[i];
        if (c == '\\' and i + 1 < pattern.len) {
            i += 1; // skip escaped char — not a meta character
            continue;
        }
        if (c == '[' and !in_class) {
            in_class = true;
            continue;
        }
        if (c == ']' and in_class) {
            in_class = false;
            continue;
        }
        if (c == '|' and !in_class) {
            if (simpleRegexMatchBranch(pattern[seg_start..i], str)) return true;
            seg_start = i + 1;
        }
    }
    return simpleRegexMatchBranch(pattern[seg_start..], str);
}

/// Match a single (no-`|`) regex branch against str.
fn simpleRegexMatchBranch(pattern: []const u8, str: []const u8) bool {
    // If pattern is anchored at start, only try from position 0
    const anchored_start = pattern.len > 0 and pattern[0] == '^';
    const anchored_end = pattern.len > 0 and pattern[pattern.len - 1] == '$' and
        (pattern.len < 2 or pattern[pattern.len - 2] != '\\');

    const effective_pat = blk: {
        const s: usize = if (anchored_start) @as(usize, 1) else 0;
        const e: usize = if (anchored_end) pattern.len - 1 else pattern.len;
        if (s > e) break :blk pattern[0..0];
        break :blk pattern[s..e];
    };

    if (anchored_start) {
        return regexMatchAt(effective_pat, str, 0, anchored_end);
    }

    // Try matching at every position
    var start: usize = 0;
    while (start <= str.len) : (start += 1) {
        if (regexMatchAt(effective_pat, str, start, anchored_end)) return true;
    }
    return false;
}

fn regexMatchAt(pat: []const u8, str: []const u8, start: usize, anchored_end: bool) bool {
    return regexMatchCore(pat, 0, str, start, anchored_end);
}

fn regexMatchCore(pat: []const u8, pi_: usize, str: []const u8, si_: usize, anchored_end: bool) bool {
    var pi = pi_;
    var si = si_;

    while (pi < pat.len) {
        // Parse current atom
        const atom_start = pi;
        const atom_end = skipAtom(pat, pi);
        if (atom_end == pi) return false; // malformed

        // Check for quantifier after atom
        const has_quant = atom_end < pat.len and
            (pat[atom_end] == '*' or pat[atom_end] == '+' or pat[atom_end] == '?');

        if (has_quant) {
            const quant = pat[atom_end];
            const rest_pi = atom_end + 1;
            const atom = pat[atom_start..atom_end];

            switch (quant) {
                '*' => {
                    // Try 0..N repetitions (greedy)
                    // Count max matches
                    var count: usize = 0;
                    var pos = si;
                    while (pos < str.len and matchAtom(atom, str[pos])) {
                        pos += 1;
                        count += 1;
                    }
                    // Try from max down to 0
                    var k: usize = count + 1;
                    while (k > 0) {
                        k -= 1;
                        if (regexMatchCore(pat, rest_pi, str, si + k, anchored_end)) return true;
                    }
                    return false;
                },
                '+' => {
                    // 1..N repetitions
                    var count: usize = 0;
                    var pos = si;
                    while (pos < str.len and matchAtom(atom, str[pos])) {
                        pos += 1;
                        count += 1;
                    }
                    if (count == 0) return false;
                    var k: usize = count + 1;
                    while (k > 1) {
                        k -= 1;
                        if (regexMatchCore(pat, rest_pi, str, si + k, anchored_end)) return true;
                    }
                    return false;
                },
                '?' => {
                    // 0 or 1
                    if (si < str.len and matchAtom(atom, str[si])) {
                        if (regexMatchCore(pat, rest_pi, str, si + 1, anchored_end)) return true;
                    }
                    return regexMatchCore(pat, rest_pi, str, si, anchored_end);
                },
                else => unreachable,
            }
        } else {
            // No quantifier — must match exactly one
            const atom = pat[atom_start..atom_end];
            if (si >= str.len) return false;
            if (!matchAtom(atom, str[si])) return false;
            pi = atom_end;
            si += 1;
        }
    }

    // Pattern consumed
    if (anchored_end) return si == str.len;
    return true; // unanchored end: partial match is ok
}

/// Skip one regex atom (char class, escape, or literal) and return new pi.
fn skipAtom(pat: []const u8, pi: usize) usize {
    if (pi >= pat.len) return pi;
    if (pat[pi] == '[') {
        // Character class — find matching ]
        var p = pi + 1;
        if (p < pat.len and pat[p] == '^') p += 1;
        if (p < pat.len and pat[p] == ']') p += 1; // ] as first char in class
        while (p < pat.len and pat[p] != ']') : (p += 1) {}
        return if (p < pat.len) p + 1 else pat.len;
    }
    if (pat[pi] == '\\' and pi + 1 < pat.len) return pi + 2;
    return pi + 1; // literal or '.'
}

/// Match a single regex atom against a character.
fn matchAtom(atom: []const u8, ch: u8) bool {
    if (atom.len == 0) return false;
    if (atom[0] == '.') return true; // any char
    if (atom[0] == '\\' and atom.len >= 2) {
        return switch (atom[1]) {
            'd' => ch >= '0' and ch <= '9',
            'D' => !(ch >= '0' and ch <= '9'),
            'w' => (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or (ch >= '0' and ch <= '9') or ch == '_',
            'W' => !((ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or (ch >= '0' and ch <= '9') or ch == '_'),
            's' => ch == ' ' or ch == '\t' or ch == '\n' or ch == '\r',
            'S' => !(ch == ' ' or ch == '\t' or ch == '\n' or ch == '\r'),
            else => ch == atom[1], // escaped literal
        };
    }
    if (atom[0] == '[') {
        return matchCharClass(atom, ch);
    }
    return ch == atom[0]; // literal
}

/// Match a character class like [abc], [a-z], [^0-9]
fn matchCharClass(atom: []const u8, ch: u8) bool {
    if (atom.len < 2) return false;
    var p: usize = 1;
    var negated = false;
    if (p < atom.len and atom[p] == '^') {
        negated = true;
        p += 1;
    }
    // Remove trailing ]
    const end = if (atom.len > 0 and atom[atom.len - 1] == ']') atom.len - 1 else atom.len;

    var matched = false;
    while (p < end) {
        const c1 = atom[p];
        if (p + 2 < end and atom[p + 1] == '-') {
            // Range: a-z
            const c2 = atom[p + 2];
            if (ch >= c1 and ch <= c2) matched = true;
            p += 3;
        } else {
            if (ch == c1) matched = true;
            p += 1;
        }
    }
    return if (negated) !matched else matched;
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test "regex: simple literal match" {
    try std.testing.expect(simpleRegexMatch("hello", "hello world"));
    try std.testing.expect(!simpleRegexMatch("xyz", "hello world"));
}

test "regex: dot matches any char" {
    try std.testing.expect(simpleRegexMatch("h.llo", "hello"));
    try std.testing.expect(simpleRegexMatch("h.llo", "hxllo"));
    try std.testing.expect(!simpleRegexMatch("h.llo", "hlo"));
}

test "regex: star quantifier" {
    try std.testing.expect(simpleRegexMatch("ab*c", "ac"));
    try std.testing.expect(simpleRegexMatch("ab*c", "abc"));
    try std.testing.expect(simpleRegexMatch("ab*c", "abbc"));
    try std.testing.expect(simpleRegexMatch("a.*c", "aXYZc"));
}

test "regex: plus quantifier" {
    try std.testing.expect(!simpleRegexMatch("ab+c", "ac"));
    try std.testing.expect(simpleRegexMatch("ab+c", "abc"));
    try std.testing.expect(simpleRegexMatch("ab+c", "abbc"));
}

test "regex: question quantifier" {
    try std.testing.expect(simpleRegexMatch("ab?c", "ac"));
    try std.testing.expect(simpleRegexMatch("ab?c", "abc"));
    try std.testing.expect(!simpleRegexMatch("ab?c", "abbc"));
}

test "regex: anchors" {
    try std.testing.expect(simpleRegexMatch("^hello$", "hello"));
    try std.testing.expect(!simpleRegexMatch("^hello$", "hello world"));
    try std.testing.expect(simpleRegexMatch("^hello", "hello world"));
    try std.testing.expect(simpleRegexMatch("world$", "hello world"));
}

test "regex: character classes" {
    try std.testing.expect(simpleRegexMatch("[abc]", "a"));
    try std.testing.expect(simpleRegexMatch("[abc]", "b"));
    try std.testing.expect(simpleRegexMatch("[abc]", "c"));
    try std.testing.expect(!simpleRegexMatch("[abc]", "d"));
    try std.testing.expect(simpleRegexMatch("[a-z]", "m"));
    try std.testing.expect(!simpleRegexMatch("[a-z]", "5"));
    try std.testing.expect(simpleRegexMatch("[^0-9]", "a"));
    try std.testing.expect(!simpleRegexMatch("[^0-9]", "5"));
}

test "regex: escape sequences" {
    try std.testing.expect(simpleRegexMatch("\\d+", "123"));
    try std.testing.expect(!simpleRegexMatch("\\d+", "abc"));
    try std.testing.expect(simpleRegexMatch("\\w+", "hello_123"));
    try std.testing.expect(simpleRegexMatch("\\s+", "   "));
}

test "regex: complex patterns" {
    try std.testing.expect(simpleRegexMatch("ERR-\\d+", "ERR-1234"));
    try std.testing.expect(!simpleRegexMatch("ERR-\\d+", "ERR-abc"));
    try std.testing.expect(simpleRegexMatch("a[bc]+d", "abccd"));
    try std.testing.expect(simpleRegexMatch("^WARN|^ERROR", "ERROR something"));
    try std.testing.expect(simpleRegexMatch("^WARN|^ERROR", "WARN something"));
    try std.testing.expect(!simpleRegexMatch("^WARN|^ERROR", "INFO something"));
}

test "glob: SQL LIKE pattern" {
    try std.testing.expect(globLike("hello%", "hello world"));
    try std.testing.expect(globLike("%world", "hello world"));
    try std.testing.expect(globLike("%ello%", "hello world"));
    try std.testing.expect(!globLike("hello%", "world"));
    try std.testing.expect(globLike("h_llo", "hello"));
    try std.testing.expect(!globLike("h_llo", "hllo"));
}