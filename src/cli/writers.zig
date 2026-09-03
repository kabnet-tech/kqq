//! writers.zig — Custom std.Io.Writer implementations.
//!
//! Provides NullCountWriter (for --count mode) and TeeWriter (for --out mode),
//! plus the sanitizeFilename helper for --split-by.

const std = @import("std");

// ─── Null-count writer: discards all output, counts newlines ─────────────────
// Used by -c / --count: runs the full executor (respecting WHERE/GROUP BY) but
// discards the JSON output and counts emitted records (one '\n' per record).

pub const NullCountWriterCtx = struct {
    count: usize = 0,
    writer: std.Io.Writer,
};

pub fn nullCountDrain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    const nc: *NullCountWriterCtx = @alignCast(@fieldParentPtr("writer", w));
    // Count newlines across buffered content and incoming data
    const buffered = w.buffer[0..w.end];
    for (buffered) |b| if (b == '\n') {
        nc.count += 1;
    };
    w.end = 0;
    var total: usize = 0;
    const slices = data[0 .. data.len - 1];
    const pattern = data[data.len - 1];
    for (slices) |bytes| {
        for (bytes) |b| if (b == '\n') {
            nc.count += 1;
        };
        total += bytes.len;
    }
    for (0..splat) |_| {
        for (pattern) |b| if (b == '\n') {
            nc.count += 1;
        };
        total += pattern.len;
    }
    return total;
}

pub fn nullCountFlush(w: *std.Io.Writer) std.Io.Writer.Error!void {
    const nc: *NullCountWriterCtx = @alignCast(@fieldParentPtr("writer", w));
    const buffered = w.buffer[0..w.end];
    for (buffered) |b| if (b == '\n') {
        nc.count += 1;
    };
    w.end = 0;
}

pub const null_count_vtable = std.Io.Writer.VTable{
    .drain = nullCountDrain,
    .flush = nullCountFlush,
};

// ─── Tee writer: forwards every write to two sub-writers ─────────────────────
// Used by --out: routes output to both stdout and a named file simultaneously.

pub const TeeWriterCtx = struct {
    a: *std.Io.Writer, // stdout
    b: *std.Io.Writer, // output file
    writer: std.Io.Writer,
};

pub fn teeDrain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
    const t: *TeeWriterCtx = @alignCast(@fieldParentPtr("writer", w));
    // Flush any data already buffered in the tee writer's own buffer
    const buffered_content = w.buffer[0..w.end];
    if (buffered_content.len > 0) {
        t.a.writeAll(buffered_content) catch return error.WriteFailed;
        t.b.writeAll(buffered_content) catch return error.WriteFailed;
        w.end = 0;
    }
    if (data.len == 0) return 0;
    const slices = data[0 .. data.len - 1];
    const pattern = data[data.len - 1];
    var total: usize = 0;
    for (slices) |bytes| {
        t.a.writeAll(bytes) catch return error.WriteFailed;
        t.b.writeAll(bytes) catch return error.WriteFailed;
        total += bytes.len;
    }
    for (0..splat) |_| {
        t.a.writeAll(pattern) catch return error.WriteFailed;
        t.b.writeAll(pattern) catch return error.WriteFailed;
        total += pattern.len;
    }
    return total;
}

pub fn teeFlush(w: *std.Io.Writer) std.Io.Writer.Error!void {
    const t: *TeeWriterCtx = @alignCast(@fieldParentPtr("writer", w));
    t.a.flush() catch return error.WriteFailed;
    t.b.flush() catch return error.WriteFailed;
}

pub const tee_vtable = std.Io.Writer.VTable{
    .drain = teeDrain,
    .flush = teeFlush,
};

// ─── Helpers for --split-by ─────────────────────────────────────────────────

/// Sanitize a string for use as a filename: replace path separators and control chars.
pub fn sanitizeFilename(name: []const u8, buf: *[512]u8) []const u8 {
    var len: usize = 0;
    for (name) |c| {
        if (len >= buf.len - 1) break;
        buf[len] = switch (c) {
            '/', '\\', '\x00' => '_',
            ':' => '_',
            else => c,
        };
        len += 1;
    }
    if (len == 0) {
        buf[0] = '_';
        len = 1;
    }
    return buf[0..len];
}