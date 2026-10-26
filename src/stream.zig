//! stream.zig — event-driven JSON tokenizer
//!
//! Reads from *std.Io.Reader and emits a stream of JsonEvents.
//! Never allocates a full document tree. Strings and numbers are returned as
//! slices that are valid until the next call to next(); callers must copy
//! them if they need them longer.
//!
//! Event sequence for {"a":[{"x":1}]}:
//!   object_start
//!   key         "a"
//!   array_start
//!   object_start
//!   key         "x"
//!   number      1
//!   object_end
//!   array_end
//!   object_end
//!   end_of_input
//!
//! The scanner enforces JSON (RFC 8259) syntax: a missing or doubled comma, a
//! trailing comma, a missing colon, a malformed number, literal or escape, a raw
//! control character inside a string, or a mismatched bracket is
//! `error.BadJson` / `BadNumber` / `BadLiteral`, and input that ends inside a
//! container or string is `error.UnexpectedEof`. Any number of top-level values
//! may follow one another (an NDJSON stream is a sequence of top-level objects).
//! `\uXXXX` escapes are decoded to UTF-8; a surrogate that is not part of a valid
//! pair decodes to U+FFFD. Bytes >= 0x80 are passed through unvalidated.

const std = @import("std");

pub const EventTag = enum {
    object_start,
    object_end,
    array_start,
    array_end,
    key,
    string,
    number,
    boolean,
    null_val,
    end_of_input,
};

/// A JSON number: its value, plus its source text so callers can keep the
/// exact literal (f64 cannot represent every integer the input may contain).
pub const Number = struct {
    value: f64,
    text: []const u8, // valid until the next call to next() — copy if needed
};

pub const Event = union(EventTag) {
    object_start,
    object_end,
    array_start,
    array_end,
    key: []const u8, // valid until the next call to next() — copy if needed
    string: []const u8,
    number: Number,
    boolean: bool,
    null_val,
    end_of_input,
};

/// Malformed-input errors. Callers that can resynchronise (NDJSON readers)
/// treat these as "skip this record"; anything else (OOM, read failure) is fatal.
pub const SyntaxError = error{ BadJson, BadNumber, BadLiteral, UnexpectedEof };

pub fn isSyntaxError(err: anyerror) bool {
    return switch (err) {
        error.BadJson, error.BadNumber, error.BadLiteral, error.UnexpectedEof => true,
        else => false,
    };
}

/// Containers nested deeper than this are rejected as BadJson. The record
/// collectors recurse once per level, so an unbounded depth would let a
/// hostile line overflow the stack.
pub const max_depth = 1024;

const INIT_STR_BUF: usize = 4096;

/// What the grammar allows next.
const State = enum {
    value, // a value (top level, after ':' or after ',' in an array)
    value_or_end, // just after '[': a value or ']'
    key, // after ',' in an object: a key
    key_or_end, // just after '{': a key or '}'
    comma_or_end, // after a value inside a container: ',' or the closer
};

/// Event-driven JSON scanner backed by *std.Io.Reader.
pub const Scanner = struct {
    const Self = @This();

    reader: *std.Io.Reader,
    allocator: std.mem.Allocator,
    str_buf: []u8, // heap-owned, grows as needed
    str_len: usize = 0,
    /// Open containers, innermost last: true = object, false = array.
    stack: std.array_list.Managed(bool),
    state: State = .value,
    /// When true, a single ',' between two top-level values is skipped, so a
    /// JSON array written one element per line without its brackets still reads.
    allow_top_level_commas: bool = false,
    /// A complete top-level value was just read (a top-level ',' may follow).
    top_value_done: bool = false,
    /// Nothing but whitespace containing a newline has been consumed since the
    /// last token. Tells `recoverAtLine` whether the offending byte starts a line.
    line_start: bool = true,

    pub fn init(allocator: std.mem.Allocator, reader: *std.Io.Reader) !Self {
        const buf = try allocator.alloc(u8, INIT_STR_BUF);
        return .{
            .reader = reader,
            .allocator = allocator,
            .str_buf = buf,
            .stack = std.array_list.Managed(bool).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.str_buf);
        self.stack.deinit();
    }

    /// Current container nesting depth (0 = between top-level values).
    pub fn depth(self: *const Self) usize {
        return self.stack.items.len;
    }

    // ── low-level byte I/O ────────────────────────────────────────────────

    fn peekByte(self: *Self) !?u8 {
        return self.reader.peekByte() catch |err| switch (err) {
            error.EndOfStream => return null,
            else => |e| return e,
        };
    }

    /// Consume `n` bytes of the current token.
    fn consume(self: *Self, n: usize) void {
        self.reader.toss(n);
        self.line_start = false;
    }

    /// All buffered bytes (at least one), or null at end of input.
    fn window(self: *Self) !?[]const u8 {
        return self.reader.peekGreedy(1) catch |err| switch (err) {
            error.EndOfStream => return null,
            else => |e| return e,
        };
    }

    fn skipWs(self: *Self) !void {
        const r = self.reader;
        while (true) {
            while (r.seek < r.end) : (r.seek += 1) {
                switch (r.buffer[r.seek]) {
                    ' ', '\t', '\r' => {},
                    '\n' => self.line_start = true,
                    else => return,
                }
            }
            r.fill(1) catch |err| switch (err) {
                error.EndOfStream => return,
                else => |e| return e,
            };
        }
    }

    /// True if reaching the next non-whitespace byte would need a refill
    /// (which may move or overwrite the bytes already consumed).
    fn nextTokenNeedsRefill(self: *const Self) bool {
        const r = self.reader;
        for (r.buffer[r.seek..r.end]) |c| {
            switch (c) {
                ' ', '\t', '\r', '\n' => {},
                else => return false,
            }
        }
        return true;
    }

    // ── string reading ────────────────────────────────────────────────────

    /// Read a string body; the opening quote has been consumed. A string that
    /// is fully buffered and has no escapes is returned as a slice of the
    /// reader's buffer (no copy); otherwise it is assembled in str_buf.
    /// Either way it stays valid only until the reader is read again.
    fn readString(self: *Self) ![]const u8 {
        const r = self.reader;
        const buffered = r.buffer[r.seek..r.end];
        const n = plainRunLen(buffered);
        if (n < buffered.len and buffered[n] == '"') {
            self.consume(n + 1);
            return buffered[0..n];
        }
        self.str_len = 0;
        try self.appendSlice(buffered[0..n]);
        self.consume(n);
        while (true) {
            const w = try self.window() orelse return error.UnexpectedEof;
            // Copy the run up to the next quote, backslash or control byte in bulk.
            const i = plainRunLen(w);
            try self.appendSlice(w[0..i]);
            self.consume(i);
            if (i == w.len) continue;
            switch (w[i]) {
                '"' => {
                    self.consume(1);
                    return self.str_buf[0..self.str_len];
                },
                '\\' => {
                    self.consume(1);
                    try self.readEscape();
                },
                else => return error.BadJson, // raw control character
            }
        }
    }

    /// Decode one escape sequence; the backslash has been consumed.
    fn readEscape(self: *Self) !void {
        const esc = try self.peekByte() orelse return error.UnexpectedEof;
        const ch: u8 = switch (esc) {
            '"' => '"',
            '\\' => '\\',
            '/' => '/',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'b' => 8,
            'f' => 12,
            'u' => {
                self.consume(1);
                return self.readUnicodeEscape();
            },
            else => return error.BadJson,
        };
        self.consume(1);
        try self.appendChar(ch);
    }

    /// Decode `XXXX` after `\u`, pairing a high surrogate with a following
    /// `\uDC00`-`\uDFFF` escape. Unpaired surrogates become U+FFFD.
    fn readUnicodeEscape(self: *Self) !void {
        var unit = try self.readHex4();
        while (true) {
            if (unit >= 0xD800 and unit <= 0xDBFF) {
                const ahead: []const u8 = self.reader.peek(2) catch |err| switch (err) {
                    error.EndOfStream => "",
                    else => |e| return e,
                };
                if (ahead.len == 2 and ahead[0] == '\\' and ahead[1] == 'u') {
                    self.consume(2);
                    const low = try self.readHex4();
                    if (low >= 0xDC00 and low <= 0xDFFF) {
                        const cp: u21 = 0x10000 + ((@as(u21, unit) - 0xD800) << 10) + (low - 0xDC00);
                        return self.appendCodepoint(cp);
                    }
                    try self.appendCodepoint(0xFFFD);
                    unit = low;
                    continue;
                }
                return self.appendCodepoint(0xFFFD);
            }
            if (unit >= 0xDC00 and unit <= 0xDFFF) return self.appendCodepoint(0xFFFD);
            return self.appendCodepoint(unit);
        }
    }

    fn readHex4(self: *Self) !u16 {
        var v: u16 = 0;
        var i: usize = 0;
        while (i < 4) : (i += 1) {
            const c = try self.peekByte() orelse return error.UnexpectedEof;
            const d: u16 = std.fmt.charToDigit(c, 16) catch return error.BadJson;
            self.consume(1);
            v = v * 16 + d;
        }
        return v;
    }

    fn appendCodepoint(self: *Self, cp: u21) !void {
        var buf: [4]u8 = undefined;
        const n = std.unicode.utf8Encode(cp, &buf) catch unreachable; // surrogates never reach here
        try self.appendSlice(buf[0..n]);
    }

    fn appendChar(self: *Self, c: u8) !void {
        try self.appendSlice(&.{c});
    }

    fn appendSlice(self: *Self, s: []const u8) !void {
        if (self.str_len + s.len > self.str_buf.len) {
            var new_len = self.str_buf.len * 2;
            while (new_len < self.str_len + s.len) new_len *= 2;
            self.str_buf = try self.allocator.realloc(self.str_buf, new_len);
        }
        @memcpy(self.str_buf[self.str_len..][0..s.len], s);
        self.str_len += s.len;
    }

    // ── number reading ────────────────────────────────────────────────────

    fn readNumber(self: *Self) !Number {
        const r = self.reader;
        const buffered = r.buffer[r.seek..r.end];
        var n: usize = 0;
        while (n < buffered.len) : (n += 1) {
            switch (buffered[n]) {
                '0'...'9', '.', 'e', 'E', '+', '-' => {},
                else => break,
            }
        }
        // Common case: the number ends inside the buffer — no copy.
        if (n < buffered.len) {
            self.consume(n);
            return parseNumberText(buffered[0..n]);
        }
        self.str_len = 0;
        while (try self.window()) |w| {
            var i: usize = 0;
            while (i < w.len) : (i += 1) {
                switch (w[i]) {
                    '0'...'9', '.', 'e', 'E', '+', '-' => {},
                    else => break,
                }
            }
            try self.appendSlice(w[0..i]);
            self.consume(i);
            if (i < w.len) break;
        }
        return parseNumberText(self.str_buf[0..self.str_len]);
    }

    fn parseNumberText(text: []const u8) !Number {
        // Fast path: a short integer literal is exact in an f64 — no float parser needed.
        if (text.len <= 15 and text.len > 0) small: {
            const neg = text[0] == '-';
            const digits = text[@intFromBool(neg)..];
            if (digits.len == 0 or (digits[0] == '0' and digits.len > 1)) break :small;
            var v: u64 = 0;
            for (digits) |c| {
                if (c < '0' or c > '9') break :small;
                v = v * 10 + (c - '0');
            }
            const f: f64 = @floatFromInt(v);
            return .{ .value = if (neg) -f else f, .text = text };
        }
        if (!isJsonNumber(text)) return error.BadNumber;
        const value = std.fmt.parseFloat(f64, text) catch return error.BadNumber;
        return .{ .value = value, .text = text };
    }

    // ── literal reading ───────────────────────────────────────────────────

    fn expectLiteral(self: *Self, word: []const u8) !void {
        for (word) |expected| {
            const got = try self.peekByte() orelse return error.UnexpectedEof;
            if (got != expected) return error.BadLiteral;
            self.consume(1);
        }
    }

    // ── structure ─────────────────────────────────────────────────────────

    /// A value just finished: decide what may follow it.
    fn afterValue(self: *Self) void {
        if (self.stack.items.len == 0) {
            self.state = .value;
            self.top_value_done = true;
        } else {
            self.state = .comma_or_end;
        }
    }

    fn open(self: *Self, is_object: bool) !void {
        if (self.stack.items.len >= max_depth) return error.BadJson;
        try self.stack.append(is_object);
        self.consume(1);
        self.state = if (is_object) .key_or_end else .value_or_end;
        self.top_value_done = false;
    }

    fn close(self: *Self) Event {
        const was_object = self.stack.pop().?;
        self.consume(1);
        self.afterValue();
        return if (was_object) .object_end else .array_end;
    }

    fn readValue(self: *Self, c: u8) !Event {
        switch (c) {
            '{' => {
                try self.open(true);
                return .object_start;
            },
            '[' => {
                try self.open(false);
                return .array_start;
            },
            '"' => {
                self.consume(1);
                const s = try self.readString();
                self.afterValue();
                return .{ .string = s };
            },
            't' => {
                try self.expectLiteral("true");
                self.afterValue();
                return .{ .boolean = true };
            },
            'f' => {
                try self.expectLiteral("false");
                self.afterValue();
                return .{ .boolean = false };
            },
            'n' => {
                try self.expectLiteral("null");
                self.afterValue();
                return .null_val;
            },
            '-', '0'...'9' => {
                const n = try self.readNumber();
                self.afterValue();
                return .{ .number = n };
            },
            else => return error.BadJson,
        }
    }

    // ── public API ────────────────────────────────────────────────────────

    /// Return the next event. On a syntax error the offending byte is left
    /// unconsumed where possible (see `recoverAtLine`).
    pub fn next(self: *Self) !Event {
        while (true) {
            try self.skipWs();
            const c = try self.peekByte() orelse {
                if (self.stack.items.len > 0) return error.UnexpectedEof;
                return .end_of_input;
            };
            switch (self.state) {
                .comma_or_end => {
                    const in_object = self.stack.items[self.stack.items.len - 1];
                    if (c == ',') {
                        self.consume(1);
                        self.state = if (in_object) .key else .value;
                        continue;
                    }
                    if (c == (if (in_object) @as(u8, '}') else ']')) return self.close();
                    return error.BadJson;
                },
                .key, .key_or_end => {
                    if (c == '}' and self.state == .key_or_end) return self.close();
                    if (c != '"') return error.BadJson;
                    self.consume(1);
                    var k = try self.readString();
                    // Normally the ':' is already buffered. If reaching it means
                    // refilling, move a zero-copy key out of the reader's buffer first.
                    if (self.nextTokenNeedsRefill() and k.ptr != self.str_buf.ptr) {
                        self.str_len = 0;
                        try self.appendSlice(k);
                        k = self.str_buf[0..self.str_len];
                    }
                    try self.skipWs();
                    const colon = try self.peekByte() orelse return error.UnexpectedEof;
                    if (colon != ':') return error.BadJson;
                    self.consume(1);
                    self.state = .value;
                    return .{ .key = k };
                },
                .value, .value_or_end => {
                    if (c == ']' and self.state == .value_or_end) return self.close();
                    if (c == ',' and self.stack.items.len == 0 and
                        self.allow_top_level_commas and self.top_value_done)
                    {
                        self.consume(1);
                        self.top_value_done = false;
                        continue;
                    }
                    return self.readValue(c);
                },
            }
        }
    }

    /// After a top-level value in newline-delimited input: true if only blanks
    /// (and, with `allow_top_level_commas`, one ',') remain on the line — so
    /// `{"a":1}}` or `{"a":1} x` is rejected the same way a line parser would.
    pub fn atLineEnd(self: *Self) !bool {
        const r = self.reader;
        while (true) {
            while (r.seek < r.end) : (r.seek += 1) {
                switch (r.buffer[r.seek]) {
                    ' ', '\t', '\r' => {},
                    '\n' => return true,
                    ',' => {
                        if (!self.allow_top_level_commas or !self.top_value_done) return false;
                        self.top_value_done = false; // the one separator is used up
                    },
                    else => return false,
                }
            }
            r.fill(1) catch |err| switch (err) {
                error.EndOfStream => return true,
                else => |e| return e,
            };
        }
    }

    /// Recover from a syntax error in newline-delimited input: drop the
    /// partial record and resume at the start of the next line. When the
    /// offending byte itself begins a line inside an unfinished record (a
    /// truncated record followed by a complete one), resume right there so
    /// the complete record is not lost.
    pub fn recoverAtLine(self: *Self) !void {
        const resume_here = self.line_start and self.stack.items.len > 0;
        if (!resume_here) {
            while (try self.window()) |w| {
                if (std.mem.indexOfScalar(u8, w, '\n')) |pos| {
                    self.reader.toss(pos + 1);
                    break;
                }
                self.reader.toss(w.len);
            }
        }
        self.stack.clearRetainingCapacity();
        self.state = .value;
        self.top_value_done = false;
        self.line_start = true;
    }

    /// Skip one complete JSON value (object, array, or scalar).
    pub fn skipValue(self: *Self) !void {
        const ev = try self.next();
        switch (ev) {
            .object_start, .array_start => {
                var d: usize = 1;
                while (d > 0) {
                    switch (try self.next()) {
                        .object_start, .array_start => d += 1,
                        .object_end, .array_end => d -= 1,
                        .end_of_input => return error.UnexpectedEof,
                        else => {},
                    }
                }
            },
            else => {},
        }
    }
};

/// Length of the leading run of `bytes` that can be copied verbatim into a
/// string: everything before the first '"', '\\' or control byte.
fn plainRunLen(bytes: []const u8) usize {
    const lanes = 16;
    const V = @Vector(lanes, u8);
    var i: usize = 0;
    while (i + lanes <= bytes.len) : (i += lanes) {
        const v: V = bytes[i..][0..lanes].*;
        const special = (v == @as(V, @splat('"'))) | (v == @as(V, @splat('\\'))) | (v < @as(V, @splat(0x20)));
        const mask: u16 = @bitCast(special);
        if (mask != 0) return i + @ctz(mask);
    }
    while (i < bytes.len) : (i += 1) {
        const c = bytes[i];
        if (c == '"' or c == '\\' or c < 0x20) break;
    }
    return i;
}

/// True if `s` is a complete JSON number (RFC 8259 grammar: no leading zeros,
/// no leading '+', digits required on both sides of '.', and after 'e').
pub fn isJsonNumber(s: []const u8) bool {
    var i: usize = 0;
    if (i < s.len and s[i] == '-') i += 1;
    if (i >= s.len) return false;
    if (s[i] == '0') {
        i += 1;
    } else if (s[i] >= '1' and s[i] <= '9') {
        while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
    } else return false;
    if (i < s.len and s[i] == '.') {
        i += 1;
        if (i >= s.len or !std.ascii.isDigit(s[i])) return false;
        while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
    }
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        i += 1;
        if (i < s.len and (s[i] == '+' or s[i] == '-')) i += 1;
        if (i >= s.len or !std.ascii.isDigit(s[i])) return false;
        while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
    }
    return i == s.len;
}

/// True if `bytes` holds exactly one JSON value (optionally surrounded by
/// whitespace) under the same rules as `Scanner`. Non-allocating; used to vet
/// lines that the raw-byte fast path emits without parsing them.
pub fn isValidJson(bytes: []const u8) bool {
    var v = Validator{ .s = bytes };
    v.ws();
    if (!v.value()) return false;
    v.ws();
    return v.i == bytes.len;
}

const Validator = struct {
    s: []const u8,
    i: usize = 0,
    depth: usize = 0,

    fn ws(v: *Validator) void {
        while (v.i < v.s.len) : (v.i += 1) {
            switch (v.s[v.i]) {
                ' ', '\t', '\r', '\n' => {},
                else => return,
            }
        }
    }

    fn peek(v: *const Validator) ?u8 {
        return if (v.i < v.s.len) v.s[v.i] else null;
    }

    fn value(v: *Validator) bool {
        const c = v.peek() orelse return false;
        return switch (c) {
            '{' => v.container('}'),
            '[' => v.container(']'),
            '"' => v.string(),
            't' => v.literal("true"),
            'f' => v.literal("false"),
            'n' => v.literal("null"),
            '-', '0'...'9' => v.number(),
            else => false,
        };
    }

    fn container(v: *Validator, closer: u8) bool {
        if (v.depth >= max_depth) return false;
        v.depth += 1;
        defer v.depth -= 1;
        v.i += 1;
        v.ws();
        if (v.peek() == closer) {
            v.i += 1;
            return true;
        }
        while (true) {
            if (closer == '}') {
                if (v.peek() != '"' or !v.string()) return false;
                v.ws();
                if (v.peek() != ':') return false;
                v.i += 1;
                v.ws();
            }
            if (!v.value()) return false;
            v.ws();
            const c = v.peek() orelse return false;
            v.i += 1;
            if (c == closer) return true;
            if (c != ',') return false;
            v.ws();
        }
    }

    fn string(v: *Validator) bool {
        v.i += 1; // opening quote
        while (v.i < v.s.len) {
            const c = v.s[v.i];
            v.i += 1;
            switch (c) {
                '"' => return true,
                '\\' => {
                    const e = v.peek() orelse return false;
                    v.i += 1;
                    switch (e) {
                        '"', '\\', '/', 'b', 'f', 'n', 'r', 't' => {},
                        'u' => {
                            if (v.i + 4 > v.s.len) return false;
                            for (v.s[v.i..][0..4]) |h| {
                                if (!std.ascii.isHex(h)) return false;
                            }
                            v.i += 4;
                        },
                        else => return false,
                    }
                },
                0x00...0x1F => return false,
                else => {},
            }
        }
        return false;
    }

    fn literal(v: *Validator, word: []const u8) bool {
        if (!std.mem.startsWith(u8, v.s[v.i..], word)) return false;
        v.i += word.len;
        return true;
    }

    fn number(v: *Validator) bool {
        const start = v.i;
        while (v.i < v.s.len) : (v.i += 1) {
            switch (v.s[v.i]) {
                '0'...'9', '.', 'e', 'E', '+', '-' => {},
                else => break,
            }
        }
        return isJsonNumber(v.s[start..v.i]);
    }
};

/// Convenience: create a Scanner from a File.
/// `read_buf` must be at least 4096 bytes and must outlive the returned Scanner.
pub fn fileScanner(allocator: std.mem.Allocator, io: std.Io, file: std.Io.File, read_buf: []u8) !Scanner {
    var fr = file.reader(io, read_buf);
    return Scanner.init(allocator, &fr.interface);
}

// ─── TESTS ───────────────────────────────────────────────────────────────────

test "simple object" {
    const src = "{\"name\":\"Alice\",\"score\":98.5,\"active\":true,\"nothing\":null}";
    var r = std.Io.Reader.fixed(src);
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();

    try std.testing.expect((try sc.next()) == .object_start);
    const k0 = try sc.next();
    try std.testing.expect(k0 == .key);
    try std.testing.expectEqualStrings("name", k0.key);
    const v0 = try sc.next();
    try std.testing.expect(v0 == .string);
    try std.testing.expectEqualStrings("Alice", v0.string);
    const k1 = try sc.next();
    try std.testing.expect(k1 == .key);
    try std.testing.expectEqualStrings("score", k1.key);
    const v1 = try sc.next();
    try std.testing.expect(v1 == .number);
    try std.testing.expectApproxEqAbs(@as(f64, 98.5), v1.number.value, 0.001);
    try std.testing.expectEqualStrings("98.5", v1.number.text);
    const k2 = try sc.next();
    try std.testing.expect(k2 == .key);
    try std.testing.expectEqualStrings("active", k2.key);
    const v2 = try sc.next();
    try std.testing.expect(v2 == .boolean and v2.boolean == true);
    const k3 = try sc.next();
    try std.testing.expect(k3 == .key);
    try std.testing.expectEqualStrings("nothing", k3.key);
    try std.testing.expect((try sc.next()) == .null_val);
    try std.testing.expect((try sc.next()) == .object_end);
    try std.testing.expect((try sc.next()) == .end_of_input);
}

test "array of objects" {
    const src = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]";
    var r = std.Io.Reader.fixed(src);
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();

    var names = std.array_list.Managed([]u8).init(std.testing.allocator);
    defer {
        for (names.items) |n| std.testing.allocator.free(n);
        names.deinit();
    }

    _ = try sc.next(); // array_start
    outer: while (true) {
        switch (try sc.next()) {
            .object_start => {
                while (true) {
                    switch (try sc.next()) {
                        .object_end => break,
                        .key => |k| {
                            if (std.mem.eql(u8, k, "name")) {
                                const vev = try sc.next();
                                try names.append(try std.testing.allocator.dupe(u8, vev.string));
                            } else try sc.skipValue();
                        },
                        else => {},
                    }
                }
            },
            .array_end => break :outer,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(usize, 2), names.items.len);
    try std.testing.expectEqualStrings("Alice", names.items[0]);
    try std.testing.expectEqualStrings("Bob", names.items[1]);
}

test "nested object navigation" {
    const src = "{\"users\":[{\"name\":\"Alice\",\"address\":{\"city\":\"Austin\"}},{\"name\":\"Bob\",\"address\":{\"city\":\"Boston\"}}]}";
    var r = std.Io.Reader.fixed(src);
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();

    _ = try sc.next(); // {
    _ = try sc.next(); // key "users"
    _ = try sc.next(); // [

    var cities = std.array_list.Managed([]u8).init(std.testing.allocator);
    defer {
        for (cities.items) |c| std.testing.allocator.free(c);
        cities.deinit();
    }

    outer: while (true) {
        switch (try sc.next()) {
            .object_start => {
                while (true) {
                    switch (try sc.next()) {
                        .object_end => break,
                        .key => |k| {
                            if (std.mem.eql(u8, k, "address")) {
                                _ = try sc.next(); // object_start
                                inner: while (true) {
                                    switch (try sc.next()) {
                                        .object_end => break :inner,
                                        .key => |ak| {
                                            if (std.mem.eql(u8, ak, "city")) {
                                                const ev = try sc.next();
                                                try cities.append(try std.testing.allocator.dupe(u8, ev.string));
                                            } else try sc.skipValue();
                                        },
                                        else => {},
                                    }
                                }
                            } else try sc.skipValue();
                        },
                        else => {},
                    }
                }
            },
            .array_end => break :outer,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(usize, 2), cities.items.len);
    try std.testing.expectEqualStrings("Austin", cities.items[0]);
    try std.testing.expectEqualStrings("Boston", cities.items[1]);
}

/// Drain a scanner over `src`; returns the first error, or null if the whole
/// input scanned cleanly.
fn scanAll(src: []const u8) !?anyerror {
    var r = std.Io.Reader.fixed(src);
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    while (true) {
        const ev = sc.next() catch |err| return @as(?anyerror, err);
        if (ev == .end_of_input) return null;
    }
}

test "truncated input is UnexpectedEof, never a hang" {
    // Regression: a record missing its closing brace used to spin forever
    // (end of input fell into a catch-all branch of the collect loop).
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedEof), try scanAll("{\"a\":1"));
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedEof), try scanAll("{\"a\":{\"b\":1}"));
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedEof), try scanAll("[1,2"));
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedEof), try scanAll("{\"a\":\"unterminated"));
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedEof), try scanAll("{\"a\""));
    try std.testing.expectEqual(@as(?anyerror, error.UnexpectedEof), try scanAll("{\"a\":"));
}

test "structural errors are rejected" {
    // Regression: `{"a":2,,}` used to be accepted.
    const bad = [_][]const u8{
        "{\"a\":2,,}",   "{\"a\":2,}",       "{,\"a\":2}",     "{\"a\" 2}",
        "{\"a\":2 \"b\":3}", "[1,]",         "[,1]",           "[1 2]",
        "{\"a\":1]",     "[1}",              "}",              "]",
        "{1:2}",         "{\"a\":01}",       "{\"a\":1.}",     "{\"a\":.5}",
        "{\"a\":-}",     "{\"a\":1e}",       "{\"a\":+1}",     "{\"a\":tru}",
        "{\"a\":nul}",   "{\"a\":\"x\\q\"}", "{\"a\":\"\\u12g4\"}", "{\"a\":\"tab\there\"}",
        "{\"a\":\"nl\nhere\"}", "{\"a\":1}}", "{\"a\":1},",   "1x",
    };
    for (bad) |src| {
        const err = try scanAll(src);
        if (err == null or !isSyntaxError(err.?)) {
            std.debug.print("accepted malformed input: {s}\n", .{src});
            return error.TestUnexpectedResult;
        }
        try std.testing.expect(!isValidJson(src));
    }
}

test "valid documents scan cleanly and validate" {
    const good = [_][]const u8{
        "{}",                         "[]",                     "{\"a\":[]}",
        "{\"a\":{\"b\":[1,{\"c\":null}]}}", " \t\n{\"a\" : 1 , \"b\" :2 }\n", "-0",
        "1e400",                      "0.5E-3",                 "\"\\u00e9\\ud83d\\ude00\\/\"",
        "[true,false,null]",          "{\"\":\"\"}",
    };
    for (good) |src| {
        try std.testing.expectEqual(@as(?anyerror, null), try scanAll(src));
        try std.testing.expect(isValidJson(src));
    }
    // Several top-level values in sequence (NDJSON) scan; isValidJson wants exactly one.
    try std.testing.expectEqual(@as(?anyerror, null), try scanAll("{\"a\":1}\n{\"a\":2}\n"));
    try std.testing.expect(!isValidJson("{\"a\":1}\n{\"a\":2}"));
}

test "unicode escapes decode to UTF-8, surrogate pairs combine" {
    // Regression: \uXXXX used to decode to '?'.
    var r = std.Io.Reader.fixed("[\"caf\\u00e9\",\"\\ud83d\\ude00\",\"\\ud800x\",\"\\udc00\",\"\\u0041\\u00\\u\"]");
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    _ = try sc.next();
    try std.testing.expectEqualStrings("café", (try sc.next()).string);
    try std.testing.expectEqualStrings("😀", (try sc.next()).string);
    try std.testing.expectEqualStrings("\u{FFFD}x", (try sc.next()).string); // lone high surrogate
    try std.testing.expectEqualStrings("\u{FFFD}", (try sc.next()).string); // lone low surrogate
    try std.testing.expectError(error.BadJson, sc.next()); // "\u00\u" — bad hex
}

test "number events keep their source text" {
    var r = std.Io.Reader.fixed("[9007199254740993,1e300,-0.5]");
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    _ = try sc.next();
    try std.testing.expectEqualStrings("9007199254740993", (try sc.next()).number.text);
    const big = (try sc.next()).number;
    try std.testing.expectEqualStrings("1e300", big.text);
    try std.testing.expectEqual(@as(f64, 1e300), big.value);
    try std.testing.expectEqual(@as(f64, -0.5), (try sc.next()).number.value);
}

test "long strings spanning reader refills are read whole" {
    const allocator = std.testing.allocator;
    var src = std.array_list.Managed(u8).init(allocator);
    defer src.deinit();
    try src.appendSlice("{\"s\":\"");
    for (0..20000) |i| try src.append('a' + @as(u8, @intCast(i % 26)));
    try src.appendSlice("\\n\"}");
    // A tiny reader buffer forces many refills mid-string.
    var buf: [7]u8 = undefined;
    var tr: std.testing.Reader = .init(&buf, &.{.{ .buffer = src.items }});
    var sc = try Scanner.init(allocator, &tr.interface);
    defer sc.deinit();
    _ = try sc.next();
    _ = try sc.next();
    const s = (try sc.next()).string;
    try std.testing.expectEqual(@as(usize, 20001), s.len);
    try std.testing.expectEqual(@as(u8, '\n'), s[20000]);
}

test "recoverAtLine resumes at the next record" {
    // A truncated record followed by a complete one: the error is detected at
    // the '{' that begins the second line, which must not be discarded.
    var r = std.Io.Reader.fixed("{\"a\":1\n{\"a\":2}\n{\"a\":3,,}\n{\"a\":4}\n");
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    var seen = std.array_list.Managed(f64).init(std.testing.allocator);
    defer seen.deinit();
    var errors: usize = 0;
    while (true) {
        const ev = sc.next() catch |err| {
            try std.testing.expect(isSyntaxError(err));
            errors += 1;
            try sc.recoverAtLine();
            continue;
        };
        switch (ev) {
            .end_of_input => break,
            .number => |n| try seen.append(n.value),
            else => {},
        }
    }
    // 1 and 3 belong to malformed records but were emitted before the error
    // surfaced; 2 and 4 come from the intact lines.
    try std.testing.expectEqualSlices(f64, &.{ 1, 2, 3, 4 }, seen.items);
    try std.testing.expectEqual(@as(usize, 2), errors);
}

test "atLineEnd rejects trailing garbage after a top-level value" {
    var r = std.Io.Reader.fixed("{\"a\":1}  \n{\"a\":2}},\n{\"a\":3},\n{\"a\":4},,\n");
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    sc.allow_top_level_commas = true;
    const want = [_]bool{ true, false, true, false };
    for (want) |w| {
        try std.testing.expect((try sc.next()) == .object_start);
        _ = try sc.next(); // key
        _ = try sc.next(); // value
        try std.testing.expect((try sc.next()) == .object_end);
        try std.testing.expectEqual(w, try sc.atLineEnd());
        if (!w) try sc.recoverAtLine();
    }
    try std.testing.expect((try sc.next()) == .end_of_input);
}

test "top-level commas only when allowed" {
    try std.testing.expect((try scanAll("{\"a\":1},\n{\"a\":2}")) != null);
    var r = std.Io.Reader.fixed("{\"a\":1},\n{\"a\":2},\n");
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    sc.allow_top_level_commas = true;
    var objects: usize = 0;
    while (true) {
        const ev = try sc.next();
        if (ev == .end_of_input) break;
        if (ev == .object_start) objects += 1;
    }
    try std.testing.expectEqual(@as(usize, 2), objects);
}

test "nesting deeper than max_depth is rejected" {
    const allocator = std.testing.allocator;
    const deep = try allocator.alloc(u8, (max_depth + 1) * 2);
    defer allocator.free(deep);
    @memset(deep[0 .. max_depth + 1], '[');
    @memset(deep[max_depth + 1 ..], ']');
    try std.testing.expectEqual(@as(?anyerror, error.BadJson), try scanAll(deep));
    try std.testing.expect(!isValidJson(deep));
    try std.testing.expectEqual(@as(?anyerror, null), try scanAll(deep[1 .. deep.len - 1]));
    try std.testing.expect(isValidJson(deep[1 .. deep.len - 1]));
}

/// Fuzz property: the scanner never hangs or crashes, agrees with the
/// non-allocating validator, and — for inputs free of the cases where kqq is
/// deliberately lenient (bytes >= 0x80, unpaired surrogate escapes) — accepts
/// exactly the documents std.json accepts.
fn fuzzScanner(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [256]u8 = undefined;
    const len = smith.sliceWeightedBytes(&buf, &.{
        .rangeAtMost(u8, 0x00, 0xff, 1),
        .rangeAtMost(u8, 0x20, 0x7e, 4),
        .value(u8, '"', 4),
        .value(u8, '\\', 3),
        .value(u8, '{', 4),
        .value(u8, '}', 4),
        .value(u8, '[', 3),
        .value(u8, ']', 3),
        .value(u8, ',', 4),
        .value(u8, ':', 4),
        .rangeAtMost(u8, '0', '9', 4),
        .value(u8, '\n', 2),
    });
    const input = buf[0..len];

    var r = std.Io.Reader.fixed(input);
    var sc = try Scanner.init(std.testing.allocator, &r);
    defer sc.deinit();
    var values: usize = 0; // top-level values started
    const accepted = while (true) {
        const ev = sc.next() catch |err| {
            if (!isSyntaxError(err)) return err;
            break false;
        };
        switch (ev) {
            .end_of_input => break true,
            .object_start, .array_start => if (sc.depth() == 1) {
                values += 1;
            },
            .string, .number, .boolean, .null_val => if (sc.depth() == 0) {
                values += 1;
            },
            .key, .object_end, .array_end => {},
        }
    };
    const one_value = accepted and values == 1;
    try std.testing.expectEqual(one_value, isValidJson(input));

    for (input) |b| if (b >= 0x80) return;
    if (std.ascii.findIgnoreCase(input, "\\ud") != null) return;
    const std_ok = std.json.validate(std.testing.allocator, input) catch false;
    try std.testing.expectEqual(std_ok, one_value);
}

test "fuzz: scanner agrees with validator and std.json" {
    try std.testing.fuzz({}, fuzzScanner, .{ .corpus = &.{
        "{\"a\":1}",
        "{\"a\":[1,2,{\"b\":null}],\"c\":\"\\u00e9\"}",
        "{\"a\":2,,}",
        "[1,]",
        "{\"a\":1",
        "-0.5e+3",
        "\"\\ud83d\\ude00\"",
    } });
}
