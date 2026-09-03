//! stream.zig — event-driven JSON tokenizer
//!
//! Reads from *std.Io.Reader and emits a stream of JsonEvents.
//! Never allocates a full document tree. Strings are returned as slices
//! into an internal grow-on-demand buffer owned by the Scanner; callers
//! must copy the slice if they need it past the next call to next().
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

pub const Event = union(EventTag) {
    object_start,
    object_end,
    array_start,
    array_end,
    key: []const u8, // slice into Scanner.str_buf — copy if needed
    string: []const u8,
    number: f64,
    boolean: bool,
    null_val,
    end_of_input,
};

const INIT_STR_BUF: usize = 4096;

/// Event-driven JSON scanner backed by *std.Io.Reader.
pub const Scanner = struct {
    const Self = @This();

    reader: *std.Io.Reader,
    allocator: std.mem.Allocator,
    str_buf: []u8, // heap-owned, grows as needed
    str_len: usize = 0,
    depth: usize = 0,
    in_object: [128]bool = [_]bool{false} ** 128,
    expect_key: bool = false,

    pub fn init(allocator: std.mem.Allocator, reader: *std.Io.Reader) !Self {
        const buf = try allocator.alloc(u8, INIT_STR_BUF);
        return .{ .reader = reader, .allocator = allocator, .str_buf = buf };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.str_buf);
    }

    // ── low-level byte I/O ────────────────────────────────────────────────

    fn peekByte(self: *Self) !?u8 {
        const s = std.Io.Reader.peek(self.reader, 1) catch |err| switch (err) {
            error.EndOfStream => return null,
            else => return err,
        };
        return if (s.len > 0) s[0] else null;
    }

    fn readByte(self: *Self) !?u8 {
        const b = try self.peekByte() orelse return null;
        std.Io.Reader.toss(self.reader, 1);
        return b;
    }

    fn skipWs(self: *Self) !void {
        while (try self.peekByte()) |c| {
            if (c == ' ' or c == '\t' or c == '\n' or c == '\r')
                std.Io.Reader.toss(self.reader, 1)
            else
                break;
        }
    }

    // ── string reading ────────────────────────────────────────────────────

    fn readString(self: *Self) ![]const u8 {
        self.str_len = 0;
        while (true) {
            const b = try self.readByte() orelse return error.UnexpectedEof;
            if (b == '"') break;
            if (b == '\\') {
                const esc = try self.readByte() orelse return error.UnexpectedEof;
                const ch: u8 = switch (esc) {
                    '"' => '"',
                    '\\' => '\\',
                    '/' => '/',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'b' => 8,
                    'f' => 12,
                    'u' => blk: {
                        var i: u4 = 0;
                        while (i < 4) : (i += 1) _ = try self.readByte();
                        break :blk '?';
                    },
                    else => esc,
                };
                try self.appendChar(ch);
            } else {
                try self.appendChar(b);
            }
        }
        return self.str_buf[0..self.str_len];
    }

    fn appendChar(self: *Self, c: u8) !void {
        if (self.str_len >= self.str_buf.len) {
            self.str_buf = try self.allocator.realloc(self.str_buf, self.str_buf.len * 2);
        }
        self.str_buf[self.str_len] = c;
        self.str_len += 1;
    }

    // ── number reading ────────────────────────────────────────────────────

    fn readNumber(self: *Self, first: u8) !f64 {
        self.str_len = 0;
        try self.appendChar(first);
        while (try self.peekByte()) |c| {
            if ((c >= '0' and c <= '9') or c == '.' or
                c == 'e' or c == 'E' or c == '+' or c == '-')
            {
                std.Io.Reader.toss(self.reader, 1);
                try self.appendChar(c);
            } else break;
        }
        return std.fmt.parseFloat(f64, self.str_buf[0..self.str_len]) catch error.BadNumber;
    }

    // ── literal reading ───────────────────────────────────────────────────

    fn expectLiteral(self: *Self, rest: []const u8) !void {
        for (rest) |expected| {
            const got = try self.readByte() orelse return error.UnexpectedEof;
            if (got != expected) return error.BadLiteral;
        }
    }

    // ── afterValue ────────────────────────────────────────────────────────

    fn afterValue(self: *Self) !void {
        try self.skipWs();
        if (try self.peekByte()) |p| {
            if (p == ',') {
                std.Io.Reader.toss(self.reader, 1);
                try self.skipWs();
                if (self.depth > 0) {
                    const d = self.depth - 1;
                    if (d < self.in_object.len and self.in_object[d])
                        self.expect_key = true;
                }
            }
        }
    }

    // ── public API ────────────────────────────────────────────────────────

    pub fn next(self: *Self) !Event {
        try self.skipWs();
        const c = try self.readByte() orelse return .end_of_input;
        switch (c) {
            '{' => {
                if (self.depth < self.in_object.len) self.in_object[self.depth] = true;
                self.depth += 1;
                self.expect_key = true;
                return .object_start;
            },
            '}' => {
                self.depth -= 1;
                self.expect_key = self.depth > 0 and
                    (if (self.depth - 1 < self.in_object.len) self.in_object[self.depth - 1] else false);
                try self.skipWs();
                if (try self.peekByte()) |p| if (p == ',') std.Io.Reader.toss(self.reader, 1);
                return .object_end;
            },
            '[' => {
                if (self.depth < self.in_object.len) self.in_object[self.depth] = false;
                self.depth += 1;
                self.expect_key = false;
                return .array_start;
            },
            ']' => {
                self.depth -= 1;
                self.expect_key = self.depth > 0 and
                    (if (self.depth - 1 < self.in_object.len) self.in_object[self.depth - 1] else false);
                try self.skipWs();
                if (try self.peekByte()) |p| if (p == ',') std.Io.Reader.toss(self.reader, 1);
                return .array_end;
            },
            '"' => {
                const s = try self.readString();
                if (self.expect_key) {
                    try self.skipWs();
                    const colon = try self.readByte() orelse return error.UnexpectedEof;
                    if (colon != ':') return error.BadJson;
                    self.expect_key = false;
                    return Event{ .key = s };
                }
                try self.afterValue();
                return Event{ .string = s };
            },
            't' => {
                try self.expectLiteral("rue");
                try self.afterValue();
                return Event{ .boolean = true };
            },
            'f' => {
                try self.expectLiteral("alse");
                try self.afterValue();
                return Event{ .boolean = false };
            },
            'n' => {
                try self.expectLiteral("ull");
                try self.afterValue();
                return .null_val;
            },
            '0'...'9', '-' => {
                const n = try self.readNumber(c);
                try self.afterValue();
                return Event{ .number = n };
            },
            ',' => {
                if (self.depth > 0 and self.depth - 1 < self.in_object.len and
                    self.in_object[self.depth - 1])
                    self.expect_key = true;
                return self.next();
            },
            else => return error.BadJson,
        }
    }

    /// Skip one complete JSON value (object, array, or scalar).
    pub fn skipValue(self: *Self) !void {
        try self.skipWs();
        _ = try self.peekByte() orelse return;
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
    try std.testing.expectApproxEqAbs(@as(f64, 98.5), v1.number, 0.001);
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
