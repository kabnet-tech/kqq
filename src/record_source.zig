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
const kq_stream_exec = @import("kq_stream_exec");

pub const OwnedValue = kq_stream_exec.OwnedValue;
pub const Record = kq_stream_exec.Record;

// ─── TextLineSource ──────────────────────────────────────────────────────────
//
// Reads lines from a reader, produces Record{ "line": <text>, "_n": <line_number> }
// Blank lines are skipped.
//
// Usage:
//   echo -e "hello\nworld" | kq --text 'select line where line contains "hello"'

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
            // Try to read a line
            const raw_line: ?[]const u8 = blk: {
                if (self.reader.takeDelimiter('\n') catch |err| switch (err) {
                    error.StreamTooLong => null,
                    else => return err,
                }) |slice| {
                    break :blk slice;
                }
                // Slow path: line longer than buffer
                self.line_buf.clearRetainingCapacity();
                while (true) {
                    if (self.reader.takeDelimiter('\n') catch |err| switch (err) {
                        error.StreamTooLong => null,
                        else => return err,
                    }) |chunk| {
                        try self.line_buf.appendSlice(chunk);
                        break;
                    }
                    if (self.line_buf.items.len == 0) {
                        self.done = true;
                        return null;
                    }
                    break;
                }
                break :blk self.line_buf.items;
            };

            const line = raw_line orelse {
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
            try rec.fields.put(self.allocator, n_key, OwnedValue{ .number = @floatFromInt(self.line_num) });

            return rec;
        }
    }
};

// ─── DelimitedSource ─────────────────────────────────────────────────────────
//
// Reads delimiter-separated lines, produces records with named columns.
// If `header = true`, the first line defines column names.
// Otherwise, columns are named f1, f2, f3, ...
// Extra columns are named by index. Missing columns become null.
//
// Usage:
//   cat data.csv | kq --delim ',' --header 'select name, age where age > 30'
//   echo "a:b:c" | kq --delim ':' --cols "x,y,z" 'select x'

pub const DelimitedSource = struct {
    reader: *std.Io.Reader,
    allocator: std.mem.Allocator,
    delimiter: u8,
    col_names: ?[][]const u8, // null = auto-generate f1, f2, ...
    col_names_owned: bool = false,
    header_parsed: bool = false,
    use_header: bool,
    line_buf: std.array_list.Managed(u8),
    line_num: usize = 0,
    done: bool = false,

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
    }

    fn readLine(self: *DelimitedSource) !?[]const u8 {
        if (self.reader.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => null,
            else => return err,
        }) |slice| {
            return slice;
        }
        // Slow path
        self.line_buf.clearRetainingCapacity();
        while (true) {
            if (self.reader.takeDelimiter('\n') catch |err| switch (err) {
                error.StreamTooLong => null,
                else => return err,
            }) |chunk| {
                try self.line_buf.appendSlice(chunk);
                break;
            }
            if (self.line_buf.items.len == 0) {
                self.done = true;
                return null;
            }
            break;
        }
        return self.line_buf.items;
    }

    fn parseHeader(self: *DelimitedSource) !void {
        self.header_parsed = true;
        if (!self.use_header) return;
        if (self.col_names != null) return; // explicit cols take precedence

        const line = try self.readLine() orelse return;
        const trimmed = std.mem.trimEnd(u8, line, "\r");
        if (trimmed.len == 0) return;

        var names = std.array_list.Managed([]const u8).init(self.allocator);
        errdefer {
            for (names.items) |n| self.allocator.free(n);
            names.deinit();
        }

        var it = std.mem.splitScalar(u8, trimmed, self.delimiter);
        while (it.next()) |col| {
            const name = std.mem.trim(u8, col, " \t\"");
            try names.append(try self.allocator.dupe(u8, name));
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

        while (true) {
            const raw_line = try self.readLine() orelse return null;
            const trimmed = std.mem.trimEnd(u8, raw_line, "\r");
            if (trimmed.len == 0) continue;

            self.line_num += 1;

            var rec = Record.init(self.allocator);
            errdefer rec.deinit();

            var col_idx: usize = 0;
            var it = std.mem.splitScalar(u8, trimmed, self.delimiter);
            while (it.next()) |col_raw| {
                const col = std.mem.trim(u8, col_raw, " \t");
                const key = try self.getColName(col_idx);
                errdefer self.allocator.free(key);

                // Try to parse as number, otherwise string
                const val: OwnedValue = if (col.len > 0)
                    if (std.fmt.parseFloat(f64, col)) |n|
                        OwnedValue{ .number = n }
                    else |_|
                        OwnedValue{ .string = try self.allocator.dupe(u8, col) }
                else
                    OwnedValue.null_val;

                try rec.fields.put(self.allocator, key, val);
                col_idx += 1;
            }

            // Add line number
            const n_key = try self.allocator.dupe(u8, "_n");
            errdefer self.allocator.free(n_key);
            try rec.fields.put(self.allocator, n_key, OwnedValue{ .number = @floatFromInt(self.line_num) });

            return rec;
        }
    }
};

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
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rec1.get("_n").?.number, 0.01);

    // Second line
    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("goodbye world", rec2.get("line").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rec2.get("_n").?.number, 0.01);

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
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rec1.get("_n").?.number, 0.01);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("second", rec2.get("line").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rec2.get("_n").?.number, 0.01);

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
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("age").?.number, 0.01);
    try std.testing.expectEqualStrings("Austin", rec1.get("city").?.string);

    // Second data row
    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("Bob", rec2.get("name").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 25.0), rec2.get("age").?.number, 0.01);
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
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("f2").?.number, 0.01);
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
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("years").?.number, 0.01);

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
    try std.testing.expectApproxEqAbs(@as(f64, 30.0), rec1.get("age").?.number, 0.01);

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
    try std.testing.expectApproxEqAbs(@as(f64, 200.0), rec1.get("code").?.number, 0.01);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectEqualStrings("web2", rec2.get("host").?.string);
    try std.testing.expectApproxEqAbs(@as(f64, 500.0), rec2.get("code").?.number, 0.01);

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
    try std.testing.expectApproxEqAbs(@as(f64, 42.0), rec.get("f1").?.number, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, 3.14), rec.get("f2").?.number, 0.01);
    try std.testing.expectApproxEqAbs(@as(f64, -7.0), rec.get("f3").?.number, 0.01);
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
    try std.testing.expectApproxEqAbs(@as(f64, 1.0), rec1.get("_n").?.number, 0.01);

    var rec2 = (try src.next()).?;
    defer rec2.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 2.0), rec2.get("_n").?.number, 0.01);

    var rec3 = (try src.next()).?;
    defer rec3.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 3.0), rec3.get("_n").?.number, 0.01);
}
