//! llm.zig — LLM streaming mode helpers, SSE parsing, schema validation.
//!
//! Helper functions for querying JSON objects as they emerge from streaming
//! LLM APIs (Ollama, OpenAI, Anthropic). The main executor functions
//! (execLlmStream, execLlmGlobalAgg, execLlmGroupBy) remain in stream_exec.zig
//! because they depend on the executor infrastructure (collectRecord, projectRecord,
//! aggregateGroupBy, etc.).

const std = @import("std");
const query = @import("kqq_query");
const record_mod = @import("kqq_record");
const output_mod = @import("kqq_output");

const Record = record_mod.Record;
const OwnedValue = record_mod.OwnedValue;
const SelectField = query.SelectField;
const writeJsonEscaped = output_mod.writeJsonEscaped;
const writeNumber = output_mod.writeNumber;
const writeRecordJson = output_mod.writeRecordJson;

// ─── API Protocol Mode ───────────────────────────────────────────────────────

pub const ApiMode = enum {
    ollama,
    openai,
    anthropic,
};

// ─── Schema Validation ──────────────────────────────────────────────────────

pub const SchemaFieldType = enum {
    string,
    number,
    boolean,
    any,
};

pub const SchemaField = struct {
    name: []const u8,
    ftype: SchemaFieldType,
    enum_values: ?[]const []const u8 = null, // for enum(a,b,c)
};

/// Parse an --expect schema string like "name:string,price:number,type:enum(sedan,truck)"
pub fn parseExpectSchema(allocator: std.mem.Allocator, spec: []const u8) ![]SchemaField {
    var fields = std.array_list.Managed(SchemaField).init(allocator);
    errdefer {
        for (fields.items) |f| {
            if (f.enum_values) |ev| allocator.free(ev);
        }
        fields.deinit();
    }

    var rest = spec;
    while (rest.len > 0) {
        // Find field name
        const colon = std.mem.indexOfScalar(u8, rest, ':') orelse return error.InvalidSchema;
        const name = rest[0..colon];
        rest = rest[colon + 1 ..];

        // Find type (up to next comma or end)
        if (std.mem.startsWith(u8, rest, "enum(")) {
            // enum(a,b,c) — find matching paren
            const open = 4; // index of '('
            const close = std.mem.indexOfScalar(u8, rest[open + 1 ..], ')') orelse return error.InvalidSchema;
            const enum_body = rest[open + 1 ..][0..close];

            // Parse comma-separated enum values
            var vals = std.array_list.Managed([]const u8).init(allocator);
            var vrest = enum_body;
            while (vrest.len > 0) {
                const vcomma = std.mem.indexOfScalar(u8, vrest, ',');
                const val = if (vcomma) |vc| vrest[0..vc] else vrest;
                try vals.append(val);
                vrest = if (vcomma) |vc| vrest[vc + 1 ..] else "";
            }

            try fields.append(.{
                .name = name,
                .ftype = .string,
                .enum_values = try vals.toOwnedSlice(),
            });

            // Advance past "enum(...)"
            const consumed = open + 1 + close + 1; // enum( + body + )
            rest = rest[consumed..];
            if (rest.len > 0 and rest[0] == ',') rest = rest[1..];
        } else {
            const comma = std.mem.indexOfScalar(u8, rest, ',');
            const type_str = if (comma) |c| rest[0..c] else rest;
            rest = if (comma) |c| rest[c + 1 ..] else "";

            const ftype: SchemaFieldType = if (std.mem.eql(u8, type_str, "string"))
                .string
            else if (std.mem.eql(u8, type_str, "number"))
                .number
            else if (std.mem.eql(u8, type_str, "boolean") or std.mem.eql(u8, type_str, "bool"))
                .boolean
            else if (std.mem.eql(u8, type_str, "any"))
                .any
            else
                return error.InvalidSchema;

            try fields.append(.{ .name = name, .ftype = ftype });
        }
    }

    return try fields.toOwnedSlice();
}

// ─── SSE (Server-Sent Events) line parsing ──────────────────────────────────
//
// SSE format:  data: {...}\n\n
// Comment:     : keep-alive\n
// Sentinel:    data: [DONE]\n\n

/// Strip SSE framing from a line. Returns the JSON payload, or null if
/// the line is a comment, empty, or the [DONE] sentinel.
pub fn stripSseLine(line: []const u8) ?[]const u8 {
    const trimmed = std.mem.trimEnd(u8, line, " \t\r");
    if (trimmed.len == 0) return null;
    if (trimmed[0] == ':') return null; // SSE comment
    if (std.mem.startsWith(u8, trimmed, "data: ")) {
        const payload = trimmed[6..];
        if (std.mem.eql(u8, payload, "[DONE]")) return null;
        return payload;
    }
    if (std.mem.startsWith(u8, trimmed, "data:")) {
        const payload = trimmed[5..];
        if (std.mem.eql(u8, payload, "[DONE]")) return null;
        return payload;
    }
    // Not SSE framed — return as-is (could be plain NDJSON)
    return trimmed;
}

// ─── Nested path extraction ─────────────────────────────────────────────────
//
// Given a parsed JSON object (as a Record with possible "raw" sub-objects)
// and a dotted path like "choices.0.delta.content", navigate into nested
// JSON to extract the leaf string value.

/// Extract a string value at a dotted path from a JSON byte slice.
/// Path segments separated by '.', numeric segments index arrays.
/// Returns the extracted string, or null if not found.
pub fn extractNestedField(allocator: std.mem.Allocator, json_bytes: []const u8, dotted_path: []const u8) ?[]const u8 {
    // Parse the JSON value
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{}) catch return null;
    defer parsed.deinit();

    var current = parsed.value;

    // Walk the path
    var rest = dotted_path;
    while (rest.len > 0) {
        const dot_pos = std.mem.indexOfScalar(u8, rest, '.');
        const segment = if (dot_pos) |d| rest[0..d] else rest;
        rest = if (dot_pos) |d| rest[d + 1 ..] else "";

        switch (current) {
            .object => |obj| {
                current = obj.get(segment) orelse return null;
            },
            .array => |arr| {
                const idx = std.fmt.parseInt(usize, segment, 10) catch return null;
                if (idx >= arr.items.len) return null;
                current = arr.items[idx];
            },
            else => return null,
        }
    }

    // Extract the leaf value as a string
    return switch (current) {
        .string => |s| allocator.dupe(u8, s) catch null,
        .number_string => |s| allocator.dupe(u8, s) catch null,
        else => null,
    };
}

// ─── Schema validation ─────────────────────────────────────────────────────

/// Validate a Record against an expect schema. Returns true if valid.
pub fn validateSchema(rec: *const Record, schema: []const SchemaField) bool {
    for (schema) |sf| {
        const val = rec.get(sf.name) orelse return false; // missing field
        switch (sf.ftype) {
            .string => {
                if (val != .string) return false;
                if (sf.enum_values) |ev| {
                    var found = false;
                    for (ev) |allowed| {
                        if (val == .string and std.mem.eql(u8, val.string, allowed)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) return false;
                }
            },
            .number => {
                if (val != .number) return false;
            },
            .boolean => {
                if (val != .boolean) return false;
            },
            .any => {},
        }
    }
    return true;
}

// ─── Brace-depth object detection ────────────────────────────────────────────

/// Scan the buffer for the first complete top-level JSON object.
/// Tracks brace depth while respecting quoted strings and escape sequences.
/// Returns the byte span [start..end) of the complete object, or null.
pub fn extractNextObject(buf: []const u8) ?struct { start: usize, end: usize } {
    // Scan forward to the first '{' (skip any leading text, whitespace, commas, brackets)
    var start: usize = 0;
    while (start < buf.len and buf[start] != '{') : (start += 1) {}
    if (start >= buf.len) return null;

    var depth: i32 = 0;
    var in_string = false;
    var i: usize = start;
    while (i < buf.len) : (i += 1) {
        const c = buf[i];
        if (in_string) {
            if (c == '\\') {
                i += 1; // skip escaped char
            } else if (c == '"') {
                in_string = false;
            }
            continue;
        }
        switch (c) {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if (depth == 0) return .{ .start = start, .end = i + 1 };
            },
            else => {},
        }
    }
    return null; // incomplete object
}

/// Remove consumed bytes from the front of the accumulator.
pub fn drainAccum(accum: *std.array_list.Managed(u8), end: usize) void {
    if (end >= accum.items.len) {
        accum.clearRetainingCapacity();
    } else {
        std.mem.copyForwards(u8, accum.items[0 .. accum.items.len - end], accum.items[end..]);
        accum.shrinkRetainingCapacity(accum.items.len - end);
    }
}

// ─── Rolling aggregate snapshots ─────────────────────────────────────────────

/// Emit a single NDJSON line with the current rolling aggregate snapshot.
/// Used by --rolling mode to show running totals after each new object.
pub fn emitAggSnapshot(
    writer: *std.Io.Writer,
    fields: []const SelectField,
    total_count: usize,
    sums: []const f64,
    mins: []const f64,
    maxs: []const f64,
    m2s: []const f64,
    n_num: []const usize,
    count_alias: []const u8,
) !void {
    try writer.writeByte('{');
    var first = true;
    if (count_alias.len > 0) {
        try writer.writeByte('"');
        try writeJsonEscaped(writer, count_alias);
        try writer.writeAll("\": ");
        try writeNumber(writer, @floatFromInt(total_count));
        first = false;
    }
    var agg_idx: usize = 0;
    for (fields) |f| {
        if (f.agg == null) continue;
        defer agg_idx += 1;
        if (!first) try writer.writeAll(", ");
        first = false;
        const name = if (f.alias) |a| a else f.key;
        try writer.writeByte('"');
        try writeJsonEscaped(writer, name);
        try writer.writeAll("\": ");
        if (n_num[agg_idx] == 0) {
            try writer.writeAll("null");
        } else {
            const val: f64 = switch (f.agg.?) {
                .sum => sums[agg_idx],
                .avg => sums[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])),
                .min => mins[agg_idx],
                .max => maxs[agg_idx],
                .variance => m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx])),
                .stddev => @sqrt(m2s[agg_idx] / @as(f64, @floatFromInt(n_num[agg_idx]))),
            };
            try writeNumber(writer, val);
        }
    }
    try writer.writeAll("}\n");
}

/// Emit a rolling group-by snapshot as a compact JSON array on one line.
/// Each group is a JSON object; the whole set is `[{...},{...}]\n`.
pub fn emitGroupBySnapshot(writer: *std.Io.Writer, groups: []Record) !void {
    try writer.writeByte('[');
    for (groups, 0..) |*rec, gi| {
        if (gi > 0) try writer.writeAll(", ");
        try writeRecordJson(writer, rec);
    }
    try writer.writeAll("]\n");
}

/// Write a single JSON object record as `{k: v, ...}` (no trailing newline or comma).
/// Used by the streaming emit path in execLlmStream.
pub fn writeOneJsonRecord(rec: *const Record, writer: *std.Io.Writer) !void {
    try writeRecordJson(writer, rec);
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test "extractNextObject: single complete object" {
    const result = extractNextObject("{\"name\":\"Alice\"}");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.start);
    try std.testing.expectEqual(@as(usize, 16), result.?.end);
}

test "extractNextObject: leading garbage before object" {
    const result = extractNextObject("garbage {\"a\":1}");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 8), result.?.start);
}

test "extractNextObject: nested braces in object" {
    const result = extractNextObject("{\"a\":{\"b\":1}}");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.start);
    try std.testing.expectEqual(@as(usize, 13), result.?.end);
}

test "extractNextObject: braces inside quoted strings" {
    const result = extractNextObject("{\"a\":\"}\"}");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.start);
    try std.testing.expectEqual(@as(usize, 9), result.?.end);
}

test "extractNextObject: escaped quotes in strings" {
    const result = extractNextObject("{\"a\":\"b\\\"c\"}");
    try std.testing.expect(result != null);
}

test "extractNextObject: incomplete object returns null" {
    const result = extractNextObject("{\"a\":1");
    try std.testing.expect(result == null);
}

test "extractNextObject: two objects back-to-back" {
    const buf = "{\"a\":1}{\"b\":2}";
    const r1 = extractNextObject(buf);
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(usize, 0), r1.?.start);
    try std.testing.expectEqual(@as(usize, 7), r1.?.end);
    const r2 = extractNextObject(buf[r1.?.end..]);
    try std.testing.expect(r2 != null);
    try std.testing.expectEqual(@as(usize, 0), r2.?.start);
    try std.testing.expectEqual(@as(usize, 7), r2.?.end);
}

test "drainAccum: removes consumed bytes" {
    const allocator = std.testing.allocator;
    var accum = std.array_list.Managed(u8).init(allocator);
    defer accum.deinit();
    try accum.appendSlice("{\"a\":1}{\"b\":2}");
    drainAccum(&accum, 7);
    try std.testing.expectEqualStrings("{\"b\":2}", accum.items);
}

test "stripSseLine: data prefix" {
    const result = stripSseLine("data: {\"id\":1}");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("{\"id\":1}", result.?);
}

test "stripSseLine: data prefix no space" {
    const result = stripSseLine("data:{\"id\":1}");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("{\"id\":1}", result.?);
}

test "stripSseLine: DONE sentinel" {
    const result = stripSseLine("data: [DONE]");
    try std.testing.expect(result == null);
}

test "stripSseLine: SSE comment" {
    const result = stripSseLine(": keep-alive");
    try std.testing.expect(result == null);
}

test "stripSseLine: empty line" {
    const result = stripSseLine("");
    try std.testing.expect(result == null);
}

test "stripSseLine: plain NDJSON passthrough" {
    const result = stripSseLine("{\"response\":\"hello\"}");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("{\"response\":\"hello\"}", result.?);
}

test "extractNestedField: simple top-level" {
    const allocator = std.testing.allocator;
    const result = extractNestedField(allocator, "{\"name\":\"Alice\"}", "name");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("Alice", result.?);
    allocator.free(result.?);
}

test "extractNestedField: nested object path" {
    const allocator = std.testing.allocator;
    const result = extractNestedField(allocator, "{\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}", "choices.0.delta.content");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("hi", result.?);
    allocator.free(result.?);
}

test "extractNestedField: array index" {
    const allocator = std.testing.allocator;
    const result = extractNestedField(allocator, "[\"a\",\"b\",\"c\"]", "1");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("b", result.?);
    allocator.free(result.?);
}

test "extractNestedField: missing path returns null" {
    const allocator = std.testing.allocator;
    const result = extractNestedField(allocator, "{\"a\":1}", "b");
    try std.testing.expect(result == null);
}

test "parseExpectSchema: basic types" {
    const allocator = std.testing.allocator;
    const schema = try parseExpectSchema(allocator, "name:string,price:number,active:boolean");
    defer {
        for (schema) |f| {
            if (f.enum_values) |ev| allocator.free(ev);
        }
        allocator.free(schema);
    }
    try std.testing.expectEqual(@as(usize, 3), schema.len);
    try std.testing.expectEqualStrings("name", schema[0].name);
    try std.testing.expectEqual(SchemaFieldType.string, schema[0].ftype);
    try std.testing.expectEqualStrings("price", schema[1].name);
    try std.testing.expectEqual(SchemaFieldType.number, schema[1].ftype);
    try std.testing.expectEqualStrings("active", schema[2].name);
    try std.testing.expectEqual(SchemaFieldType.boolean, schema[2].ftype);
}

test "parseExpectSchema: enum type" {
    const allocator = std.testing.allocator;
    const schema = try parseExpectSchema(allocator, "type:enum(sedan,truck,suv)");
    defer {
        for (schema) |f| {
            if (f.enum_values) |ev| allocator.free(ev);
        }
        allocator.free(schema);
    }
    try std.testing.expectEqual(@as(usize, 1), schema.len);
    try std.testing.expectEqualStrings("type", schema[0].name);
    try std.testing.expect(schema[0].enum_values != null);
    try std.testing.expectEqual(@as(usize, 3), schema[0].enum_values.?.len);
    try std.testing.expectEqualStrings("sedan", schema[0].enum_values.?[0]);
    try std.testing.expectEqualStrings("truck", schema[0].enum_values.?[1]);
    try std.testing.expectEqualStrings("suv", schema[0].enum_values.?[2]);
}

test "validateSchema: valid record" {
    const allocator = std.testing.allocator;
    var rec = Record.init(allocator);
    defer rec.deinit();
    _ = rec.fields.put(allocator, try allocator.dupe(u8, "name"), .{ .string = try allocator.dupe(u8, "Alice") }) catch {};
    _ = rec.fields.put(allocator, try allocator.dupe(u8, "price"), .{ .number = 30000 }) catch {};
    const schema = [_]SchemaField{
        .{ .name = "name", .ftype = .string },
        .{ .name = "price", .ftype = .number },
    };
    try std.testing.expect(validateSchema(&rec, &schema));
}

test "validateSchema: type mismatch" {
    const allocator = std.testing.allocator;
    var rec = Record.init(allocator);
    defer rec.deinit();
    _ = rec.fields.put(allocator, try allocator.dupe(u8, "name"), .{ .number = 42 }) catch {};
    const schema = [_]SchemaField{
        .{ .name = "name", .ftype = .string },
    };
    try std.testing.expect(!validateSchema(&rec, &schema));
}

test "validateSchema: missing field" {
    const allocator = std.testing.allocator;
    var rec = Record.init(allocator);
    defer rec.deinit();
    _ = rec.fields.put(allocator, try allocator.dupe(u8, "name"), .{ .string = try allocator.dupe(u8, "Alice") }) catch {};
    const schema = [_]SchemaField{
        .{ .name = "name", .ftype = .string },
        .{ .name = "price", .ftype = .number },
    };
    try std.testing.expect(!validateSchema(&rec, &schema));
}

test "validateSchema: enum valid value" {
    const allocator = std.testing.allocator;
    var rec = Record.init(allocator);
    defer rec.deinit();
    const enum_vals = [_][]const u8{ "sedan", "truck", "suv" };
    _ = rec.fields.put(allocator, try allocator.dupe(u8, "type"), .{ .string = try allocator.dupe(u8, "sedan") }) catch {};
    const schema = [_]SchemaField{
        .{ .name = "type", .ftype = .string, .enum_values = @constCast(&enum_vals) },
    };
    try std.testing.expect(validateSchema(&rec, &schema));
}

test "validateSchema: enum invalid value" {
    const allocator = std.testing.allocator;
    var rec = Record.init(allocator);
    defer rec.deinit();
    const enum_vals = [_][]const u8{ "sedan", "truck", "suv" };
    _ = rec.fields.put(allocator, try allocator.dupe(u8, "type"), .{ .string = try allocator.dupe(u8, "wagon") }) catch {};
    const schema = [_]SchemaField{
        .{ .name = "type", .ftype = .string, .enum_values = @constCast(&enum_vals) },
    };
    try std.testing.expect(!validateSchema(&rec, &schema));
}