//! output.zig — Output formatting for Records.
//!
//! Handles emitting Records in various formats: JSON array, NDJSON, CSV/TSV,
//! raw values. Used by the streaming and buffered executors.

const std = @import("std");
const record_mod = @import("kq_record");

const Record = record_mod.Record;
const OwnedValue = record_mod.OwnedValue;

// ─── Output format enum ──────────────────────────────────────────────────────

pub const OutputFormat = enum { json, csv, tsv, raw, ndjson };

// ─── Shared helpers ──────────────────────────────────────────────────────────

/// Write a number as `{d}` to a writer. Used everywhere numbers are emitted.
pub fn writeNumber(writer: *std.Io.Writer, n: f64) !void {
    var tmp: [64]u8 = undefined;
    const ns = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch "0";
    try writer.writeAll(ns);
}

/// Write a Record as a JSON object `{ "key": value, ... }` (no trailing newline).
/// Uses `writeOwnedValue` for each value. This is the single canonical record
/// emitter — all call sites should use this instead of inlining the iterator loop.
pub fn writeRecordJson(writer: *std.Io.Writer, rec: *const Record) !void {
    try writer.writeByte('{');
    var it = rec.fields.iterator();
    var first = true;
    while (it.next()) |e| {
        if (!first) try writer.writeAll(", ");
        first = false;
        try writer.writeByte('"');
        try writeJsonEscaped(writer, e.key_ptr.*);
        try writer.writeAll("\": ");
        try writeOwnedValue(writer, e.value_ptr.*);
    }
    try writer.writeByte('}');
}

// ─── Emit a single Record as one NDJSON line ──────────────────────────────────
//
// Used by --split-by to write individual records to per-value output files.

pub fn emitRecordNdjsonLine(rec: *const Record, writer: *std.Io.Writer) !void {
    try writeRecordJson(writer, rec);
    try writer.writeAll("\n");
}

/// Emit a projected Record in streaming-emit mode (no ORDER BY / DISTINCT / CSV).
/// Handles json and ndjson formats only; csv/tsv are unreachable here.
pub fn emitStreamingRecord(writer: *std.Io.Writer, rec: *Record, fmt: OutputFormat, json_first: *bool) !void {
    switch (fmt) {
        .json => {
            if (json_first.*) {
                try writer.writeAll("[\n  ");
                json_first.* = false;
            } else try writer.writeAll(",\n  ");
            try writeRecordJson(writer, rec);
        },
        .ndjson => {
            try writeRecordJson(writer, rec);
            try writer.writeByte('\n');
            try writer.flush();
        },
        .raw => {
            var it = rec.fields.iterator();
            if (it.next()) |e| try writeOwnedValueRaw(writer, e.value_ptr.*);
            try writer.writeByte('\n');
            try writer.flush();
        },
        .csv, .tsv => unreachable,
    }
}

/// Write a single OwnedValue as JSON (no trailing newline).
pub fn writeOwnedValue(writer: *std.Io.Writer, v: OwnedValue) !void {
    switch (v) {
        .string => |s| {
            try writer.writeByte('"');
            try writeJsonEscaped(writer, s);
            try writer.writeByte('"');
        },
        .number => |n| try writeNumber(writer, n),
        .boolean => |b| try writer.writeAll(if (b) "true" else "false"),
        .null_val => try writer.writeAll("null"),
        .raw => |r| try writer.writeAll(r),
    }
}

/// Write a single OwnedValue as a raw unquoted value (for --raw mode).
pub fn writeOwnedValueRaw(writer: *std.Io.Writer, v: OwnedValue) !void {
    switch (v) {
        .string => |s| try writer.writeAll(s),
        .number => |n| try writeNumber(writer, n),
        .boolean => |b| try writer.writeAll(if (b) "true" else "false"),
        .null_val => try writer.writeAll("null"),
        .raw => |r| try writer.writeAll(r),
    }
}

// ─── Emit a slice of Records to writer ──────────────────────────────────────
//
// raw=true + single projected field → one value per line (no JSON wrapper)
// otherwise → JSON array of objects

pub fn emitResults(
    records: []Record,
    count: usize,
    writer: *std.Io.Writer,
    fmt: OutputFormat,
    raw: bool,
) !void {
    const effective_fmt = if (raw) OutputFormat.raw else fmt;

    // CSV/TSV output: emit header + rows
    if (effective_fmt == .csv or effective_fmt == .tsv) {
        const sep: u8 = if (effective_fmt == .csv) ',' else '\t';
        // Collect ordered column names from first record
        if (count == 0) return;
        // Build header from first record's keys (insertion order not guaranteed by HashMap,
        // but we output in iterator order consistently)
        var header_keys = std.array_list.Managed([]const u8).init(records[0].allocator);
        defer header_keys.deinit();
        {
            var it = records[0].fields.iterator();
            while (it.next()) |e| try header_keys.append(e.key_ptr.*);
        }
        // Write header
        for (header_keys.items, 0..) |k, i| {
            if (i > 0) try writer.writeByte(sep);
            try writeCsvField(writer, k, sep);
        }
        try writer.writeByte('\n');
        // Write rows
        for (records[0..count]) |*rec| {
            for (header_keys.items, 0..) |k, i| {
                if (i > 0) try writer.writeByte(sep);
                if (rec.get(k)) |v| {
                    switch (v) {
                        .string => |s| try writeCsvField(writer, s, sep),
                        .number => |n| try writeNumber(writer, n),
                        .boolean => |b| try writer.writeAll(if (b) "true" else "false"),
                        .null_val => {}, // empty cell
                        .raw => |r| try writeCsvField(writer, r, sep),
                    }
                }
            }
            try writer.writeByte('\n');
        }
        return;
    }

    // Raw mode: emit each record's single field value as plain text
    if (effective_fmt == .raw) {
        for (records[0..count]) |*rec| {
            // Pick the first (and ideally only) field value
            var it = rec.fields.iterator();
            if (it.next()) |e| {
                try writeOwnedValueRaw(writer, e.value_ptr.*);
                try writer.writeByte('\n');
            }
        }
        return;
    }

    // NDJSON output: one object per line, no array wrapper
    if (effective_fmt == .ndjson) {
        for (records[0..count]) |*rec| {
            try writeRecordJson(writer, rec);
            try writer.writeByte('\n');
        }
        return;
    }

    // Normal JSON array output
    try writer.writeAll("[\n");
    for (records[0..count], 0..) |*rec, i| {
        try writer.writeAll("  ");
        try writeRecordJson(writer, rec);
        if (i + 1 < count) try writer.writeAll(",\n") else try writer.writeByte('\n');
    }
    try writer.writeAll("]\n");
}

/// Write a CSV/TSV field, quoting if necessary (for CSV: quote if contains comma, quote, or newline).
pub fn writeCsvField(writer: *std.Io.Writer, s: []const u8, sep: u8) !void {
    const needs_quote = sep == ',' and (std.mem.indexOfAny(u8, s, ",\"\n\r") != null);
    if (needs_quote) {
        try writer.writeByte('"');
        for (s) |c| {
            if (c == '"') try writer.writeByte('"'); // double-quote escape
            try writer.writeByte(c);
        }
        try writer.writeByte('"');
    } else {
        try writer.writeAll(s);
    }
}

/// Write a JSON-escaped string (handles backslash, quotes, control chars).
pub fn writeJsonEscaped(writer: *std.Io.Writer, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            0x00...0x08, 0x0B...0x0C, 0x0E...0x1F => {
                var buf: [6]u8 = undefined;
                const hex = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch continue;
                try writer.writeAll(hex);
            },
            else => try writer.writeByte(c),
        }
    }
}