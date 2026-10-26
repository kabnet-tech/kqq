//! record_source.zig — Format-agnostic record source abstraction
//!
//! Provides multiple RecordSource implementations that all produce Records.
//! The query engine (WHERE / SELECT / ORDER BY / LIMIT / GROUP BY / aggregates)
//! operates on Records regardless of their origin.
//!
//! Sources:
//!   - TextLineSource:      plain text lines → {line: "...", _n: N}
//!   - DelimitedSource:     CSV/TSV/custom-delimited → {col1: "...", col2: "..."}
//!
//! JSON sources (JsonNdjsonSource, JsonScopedSource, LlmAccumSource) remain in
//! stream_exec.zig because they depend on stream.Scanner internals.
//!
//! This module is self-contained: OwnedValue/Record are imported from the
//! stream_exec module and re-exported for tests. When used from stream_exec
//! or main, the caller already has the canonical Record type and the sources
//! here produce records of that same type.

const std = @import("std");
const kqq_stream_exec = @import("kqq_stream_exec");

pub const OwnedValue = kqq_stream_exec.OwnedValue;
pub const Number = kqq_stream_exec.Number;
const isJsonNumber = kqq_stream_exec.isJsonNumber;
pub const Record = kqq_stream_exec.Record;

// ─── TextLineSource ──────────────────────────────────────────────────────────
//
// Reads lines from a reader, produces Record{ "line": <text>, "_n": <line_number> }
// Blank lines are skipped.
//
// Usage:
//   echo -e "hello\nworld" | kqq --text 'select line where line contains "hello"'

pub const TextLineSource = struct {
    reader: *std.Io.Reader,
    allocator: std.mem.Allocator,
    line_num: usize = 0,
    line_buf: std.array_list.Managed(u8),
    done: bool = false,

    pub fn init(allocator: std.mem.Allocator, reader: *std.Io.Reader) TextLineSource {
        return .{
            .reader = reader,
            .allocator = allocator,
            .line_buf = std.array_list.Managed(u8).init(allocator),
        };
    }

    pub fn deinit(self: *TextLineSource) void {
        self.line_buf.deinit();
    }

    pub fn next(self: *TextLineSource) !?Record {
        if (self.done) return null;

        while (true) {
            const line = try readLine(self.reader, &self.line_buf) orelse {
                self.done = true;
                return null;
            };

            const trimmed = std.mem.trimEnd(u8, line, "\r");

            // Skip blank lines
            if (trimmed.len == 0) continue;

            self.line_num += 1;

            // Build record
            var rec = Record.init(self.allocator);
            errdefer rec.deinit();

            const line_key = try self.allocator.dupe(u8, "line");
            errdefer self.allocator.free(line_key);
            const line_val = OwnedValue{ .string = try self.allocator.dupe(u8, trimmed) };
            try rec.fields.put(self.allocator, line_key, line_val);

            const n_key = try self.allocator.dupe(u8, "_n");
            errdefer self.allocator.free(n_key);
            try rec.fields.put(self.allocator, n_key, OwnedValue.num(@floatFromInt(self.line_num)));

            return rec;
        }
    }
};

/// Read one line (without its '\n'), or null at end of input. Lines that fit
/// the reader's buffer are returned zero-copy; longer ones are assembled in
/// `long_buf`. Either way the slice is valid until the next call.
fn readLine(reader: *std.Io.Reader, long_buf: *std.array_list.Managed(u8)) !?[]const u8 {
    if (reader.takeDelimiter('\n')) |line| {
        return line; // null at end of input
    } else |err| switch (err) {
        error.StreamTooLong => {},
        else => |e| return e,
    }
    // StreamTooLong leaves the bytes buffered: accumulate via peek + toss
    // until the newline (or end of input).
    long_buf.clearRetainingCapacity();
    while (true) {
        const window = reader.peekGreedy(1) catch |err| switch (err) {
            error.EndOfStream => break,
            else => |e| return e,
        };
        if (std.mem.indexOfScalar(u8, window, '\n')) |pos| {
            try long_buf.appendSlice(window[0..pos]);
            reader.toss(pos + 1);
            break;
        }
        try long_buf.appendSlice(window);
        reader.toss(window.len);
    }
    return long_buf.items;
}

// ─── DelimitedSource ─────────────────────────────────────────────────────────
//
// Reads delimiter-separated records, produces records with named columns.
// If `header = true`, the first record defines column names.
// Otherwise, columns are named f1, f2, f3, ...
// Extra columns are named by index. Missing columns become null.
//
// Fields follow RFC 4180 quoting: a field that starts with `"` may contain the
// delimiter, `""` (an escaped quote) and newlines, up to the closing `"`.
// Quoting only encodes text: "100" and 100 are both the number 100, while an
// empty quoted field is the empty string and an empty unquoted field is null.
//
// Usage:
//   cat data.csv | kqq --delim ',' --header 'select name, age where age > 30'
//   echo "a:b:c" | kqq --delim ':' --cols "x,y,z" 'select x'

pub const DelimitedSource = struct {
    reader: *std.Io.Reader,
    allocator: std.mem.Allocator,
    delimiter: u8,
    col_names: ?[][]const u8, // null = auto-generate f1, f2, ...
    col_names_owned: bool = false,
    header_parsed: bool = false,
    use_header: bool,
    line_buf: std.array_list.Managed(u8),
    /// A record that spans lines (a quoted field containing newlines).
    record_buf: std.array_list.Managed(u8),
    /// Decoded field text of the current record, and each field's span in it.
    text_buf: std.array_list.Managed(u8),
    fields: std.array_list.Managed(Field),
    line_num: usize = 0,
    done: bool = false,

    const Field = struct { start: usize, end: usize, quoted: bool };

    pub fn init(
        allocator: std.mem.Allocator,
        reader: *std.Io.Reader,
        delimiter: u8,
        use_header: bool,
        explicit_cols: ?[]const []const u8,
    ) DelimitedSource {
        return .{
            .reader = reader,
            .allocator = allocator,
            .delimiter = delimiter,
            .col_names = if (explicit_cols) |ec| @constCast(ec) else null,
            .use_header = use_header,
            .line_buf = std.array_list.Managed(u8).init(allocator),
            .record_buf = std.array_list.Managed(u8).init(allocator),
            .text_buf = std.array_list.Managed(u8).init(allocator),
            .fields = std.array_list.Managed(Field).init(allocator),
        };
    }

    pub fn deinit(self: *DelimitedSource) void {
        if (self.col_names_owned) {
            if (self.col_names) |names| {
                for (names) |n| self.allocator.free(n);
                self.allocator.free(names);
            }
        }
        self.line_buf.deinit();
        self.record_buf.deinit();
        self.text_buf.deinit();
        self.fields.deinit();
    }

    /// Read the next non-blank record and split it into `fields`.
    /// Returns false at end of input.
    fn readRecord(self: *DelimitedSource) !bool {
        while (true) {
            const first = try readLine(self.reader, &self.line_buf) orelse {
                self.done = true;
                return false;
            };
            const line = std.mem.trimEnd(u8, first, "\r");
            if (line.len == 0) continue;
            if (!try self.splitFields(line)) {
                // A quoted field is still open: the record continues on the
                // next line(s), joined by the newline the line split removed.
                // (At end of input the unterminated field keeps what it has.)
                self.record_buf.clearRetainingCapacity();
                try self.record_buf.appendSlice(line);
                while (try readLine(self.reader, &self.line_buf)) |more| {
                    try self.record_buf.append('\n');
                    try self.record_buf.appendSlice(std.mem.trimEnd(u8, more, "\r"));
                    if (try self.splitFields(self.record_buf.items)) break;
                }
            }
            return true;
        }
    }

    fn isPad(self: *const DelimitedSource, c: u8) bool {
        return (c == ' ' or c == '\t') and c != self.delimiter;
    }

    /// Split `line` into decoded fields. Returns false when it ends inside a
    /// quoted field (that last field then holds the text read so far).
    fn splitFields(self: *DelimitedSource, line: []const u8) !bool {
        self.fields.clearRetainingCapacity();
        self.text_buf.clearRetainingCapacity();
        var i: usize = 0;
        while (true) {
            while (i < line.len and self.isPad(line[i])) i += 1;
            const start = self.text_buf.items.len;
            var quoted = false;
            if (i < line.len and line[i] == '"') {
                quoted = true;
                i += 1;
                while (true) {
                    if (i >= line.len) {
                        // still inside quotes
                        try self.fields.append(.{ .start = start, .end = self.text_buf.items.len, .quoted = true });
                        return false;
                    }
                    const c = line[i];
                    i += 1;
                    if (c == '"') {
                        if (i < line.len and line[i] == '"') {
                            try self.text_buf.append('"');
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    try self.text_buf.append(c);
                }
                // Text between the closing quote and the delimiter is kept
                // verbatim (lenient, like Python's csv module), minus padding.
                const tail_start = i;
                while (i < line.len and line[i] != self.delimiter) i += 1;
                try self.text_buf.appendSlice(std.mem.trim(u8, line[tail_start..i], " \t"));
            } else {
                const field_start = i;
                while (i < line.len and line[i] != self.delimiter) i += 1;
                var field = line[field_start..i];
                while (field.len > 0 and self.isPad(field[field.len - 1])) field.len -= 1;
                try self.text_buf.appendSlice(field);
            }
            try self.fields.append(.{ .start = start, .end = self.text_buf.items.len, .quoted = quoted });
            if (i >= line.len) return true;
            i += 1; // the delimiter: another field follows (possibly empty)
        }
    }

    fn fieldText(self: *const DelimitedSource, f: Field) []const u8 {
        return self.text_buf.items[f.start..f.end];
    }

    fn parseHeader(self: *DelimitedSource) !void {
        self.header_parsed = true;
        if (!self.use_header) return;
        if (self.col_names != null) return; // explicit cols take precedence
        if (!try self.readRecord()) return;

        var names = std.array_list.Managed([]const u8).init(self.allocator);
        errdefer {
            for (names.items) |n| self.allocator.free(n);
            names.deinit();
        }
        for (self.fields.items) |f| {
            try names.append(try self.allocator.dupe(u8, self.fieldText(f)));
        }

        self.col_names = try names.toOwnedSlice();
        self.col_names_owned = true;
    }

    fn getColName(self: *DelimitedSource, idx: usize) ![]const u8 {
        if (self.col_names) |names| {
            if (idx < names.len) return try self.allocator.dupe(u8, names[idx]);
        }
        // Auto-generate: f1, f2, f3...
        return try std.fmt.allocPrint(self.allocator, "f{d}", .{idx + 1});
    }

    pub fn next(self: *DelimitedSource) !?Record {
        if (self.done) return null;

        // Parse header on first call if needed
        if (!self.header_parsed) {
            try self.parseHeader();
        }

        if (!try self.readRecord()) return null;
        self.line_num += 1;

        var rec = Record.init(self.allocator);
        errdefer rec.deinit();

        for (self.fields.items, 0..) |f, col_idx| {
            const col = self.fieldText(f);
            const key = try self.getColName(col_idx);
            errdefer self.allocator.free(key);

            // Numbers are inferred from the text; an empty unquoted cell is null.
            const val: OwnedValue = if (col.len == 0 and !f.quoted)
                .null_val
            else if (parseNumber(col)) |n|
                // Keep the exact digits only when they are valid JSON (not "007", "+5").
                .{ .number = if (isJsonNumber(col)) try Number.fromLiteral(self.allocator, n, col) else Number.of(n) }
            else
                .{ .string = try self.allocator.dupe(u8, col) };
            errdefer val.deinit(self.allocator);

            try rec.fields.put(self.allocator, key, val);
        }

        // Add line number
        const n_key = try self.allocator.dupe(u8, "_n");
        errdefer self.allocator.free(n_key);
        try rec.fields.put(self.allocator, n_key, OwnedValue.num(@floatFromInt(self.line_num)));

        return rec;
    }
};

/// The value of a plain decimal number cell, else null.
fn parseNumber(s: []const u8) ?f64 {
    if (!looksNumeric(s)) return null;
    return std.fmt.parseFloat(f64, s) catch null;
}

/// A plain decimal number: optional sign, digits with an optional fraction,
/// optional exponent. Unlike std.fmt.parseFloat this rejects "nan", "inf",
/// hex and digit separators, which in a CSV cell are text.
fn looksNumeric(s: []const u8) bool {
    var i: usize = 0;
    if (i < s.len and (s[i] == '+' or s[i] == '-')) i += 1;
    var digits: usize = 0;
    while (i < s.len and std.ascii.isDigit(s[i])) : (i += 1) digits += 1;
    if (i < s.len and s[i] == '.') {
        i += 1;
        while (i < s.len and std.ascii.isDigit(s[i])) : (i += 1) digits += 1;
    }
    if (digits == 0) return false;
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        i += 1;
        if (i < s.len and (s[i] == '+' or s[i] == '-')) i += 1;
        const exp_start = i;
        while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
        if (i == exp_start) return false;
    }
    return i == s.len;
}

// ─── TESTS ───────────────────────────────────────────────────────────────────

test "TextLineSource: basic line reading" {
    const allocator = std.testing.allocator;
    const input = "hello world\ngoodbye world\n";
    var reader = std.Io.Reader.fixed(input);

    var src = TextLineSource.init(allocator, &reader);
    defer src.deinit();

    // First line
    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("hello world", rec1.get("line").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rec1.get("_n").?.number.f, 0.01);

    // Second line
    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("goodbye world", rec2.get("line").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rec2.get("_n").?.number.f, 0.01);

    // EOF
    try std.testing.expect(try src.next() == null);
}

test "TextLineSource: skips blank lines" {
    const allocator = std.testing.allocator;
    const input = "first\n\n\nsecond\n";
    var reader = std.Io.Reader.fixed(input);

    var src = TextLineSource.init(allocator, &reader);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("first", rec1.get("line").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rec1.get("_n").?.number.f, 0.01);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("second", rec2.get("line").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rec2.get("_n").?.number.f, 0.01);

    try std.testing.expect(try src.next() == null);
}

test "TextLineSource: handles CR-LF" {
    const allocator = std.testing.allocator;
    const input = "line one\r\nline two\r\n";
    var reader = std.Io.Reader.fixed(input);

    var src = TextLineSource.init(allocator, &reader);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("line one", rec1.get("line").?.string);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("line two", rec2.get("line").?.string);

    try std.testing.expect(try src.next() == null);
}

test "TextLineSource: empty input returns null" {
    const allocator = std.testing.allocator;
    const input = "";
    var reader = std.Io.Reader.fixed(input);

    var src = TextLineSource.init(allocator, &reader);
    defer src.deinit();

    try std.testing.expect(try src.next() == null);
}

test "TextLineSource: single line without newline" {
    const allocator = std.testing.allocator;
    const input = "just one line";
    var reader = std.Io.Reader.fixed(input);

    var src = TextLineSource.init(allocator, &reader);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("just one line", rec1.get("line").?.string);

    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: CSV with header" {
    const allocator = std.testing.allocator;
    const input = "name,age,city\nAlice,30,Austin\nBob,25,Boston\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, ',', true, null);
    defer src.deinit();

    // First data row
    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("Alice", rec1.get("name").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("age").?.number.f, 0.01);
    try std.testing.expectEqualStrings("Austin", rec1.get("city").?.string);

    // Second data row
    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("Bob", rec2.get("name").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 25.0), rec2.get("age").?.number.f, 0.01);
    try std.testing.expectEqualStrings("Boston", rec2.get("city").?.string);

    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: auto-numbered columns without header" {
    const allocator = std.testing.allocator;
    const input = "Alice,30,Austin\nBob,25,Boston\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, ',', false, null);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("Alice", rec1.get("f1").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("f2").?.number.f, 0.01);
    try std.testing.expectEqualStrings("Austin", rec1.get("f3").?.string);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("Bob", rec2.get("f1").?.string);

    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: explicit column names" {
    const allocator = std.testing.allocator;
    const input = "Alice,30\nBob,25\n";
    var reader = std.Io.Reader.fixed(input);

    const cols = [_][]const u8{ "person", "years" };
    var src = DelimitedSource.init(allocator, &reader, ',', false, &cols);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("Alice", rec1.get("person").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("years").?.number.f, 0.01);

    var rec2 = (try src.next()).?;
    rec2.deinit();
    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: TSV tab-separated" {
    const allocator = std.testing.allocator;
    const input = "name\tage\nAlice\t30\nBob\t25\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, '\t', true, null);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("Alice", rec1.get("name").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("age").?.number.f, 0.01);

    var rec2 = (try src.next()).?;
    rec2.deinit();
    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: pipe-separated" {
    const allocator = std.testing.allocator;
    const input = "host|status|code\nweb1|ok|200\nweb2|err|500\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, '|', true, null);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("web1", rec1.get("host").?.string);
    try std.testing.expectEqualStrings("ok", rec1.get("status").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 200.0), rec1.get("code").?.number.f, 0.01);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("web2", rec2.get("host").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 500.0), rec2.get("code").?.number.f, 0.01);

    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: empty fields become null" {
    const allocator = std.testing.allocator;
    const input = "a,,c\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, ',', false, null);
    defer src.deinit();

    var rec = (try src.next()).?;
    defer rec.deinit();
    try std.testing.expectEqualStrings("a", rec.get("f1").?.string);
    try std.testing.expect(rec.get("f2").? == .null_val);
    try std.testing.expectEqualStrings("c", rec.get("f3").?.string);
}

test "DelimitedSource: skips blank lines" {
    const allocator = std.testing.allocator;
    const input = "a,b\n\nc,d\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, ',', false, null);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectEqualStrings("a", rec1.get("f1").?.string);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("c", rec2.get("f1").?.string);

    try std.testing.expect(try src.next() == null);
}

test "DelimitedSource: numeric parsing" {
    const allocator = std.testing.allocator;
    const input = "42,3.14,-7,hello\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, ',', false, null);
    defer src.deinit();

    var rec = (try src.next()).?;
    defer rec.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 42.0), rec.get("f1").?.number.f, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), rec.get("f2").?.number.f, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, -7.0), rec.get("f3").?.number.f, 0.01);
    try std.testing.expectEqualStrings("hello", rec.get("f4").?.string);
}

test "DelimitedSource: has _n line number" {
    const allocator = std.testing.allocator;
    const input = "a\nb\nc\n";
    var reader = std.Io.Reader.fixed(input);

    var src = DelimitedSource.init(allocator, &reader, ',', false, null);
    defer src.deinit();

    var rec1 = (try src.next()).?;
    defer rec1.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rec1.get("_n").?.number.f, 0.01);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rec2.get("_n").?.number.f, 0.01);

    var rec3 = (try src.next()).?;
    defer rec3.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), rec3.get("_n").?.number.f, 0.01);
}
