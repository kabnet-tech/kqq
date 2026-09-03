//! record.zig — Core record types and operations.
//!
//! A Record is a flat map of field-name → OwnedValue, fully owned.
//! String values are heap-duplicated; numbers/booleans are inline.
//! Nested objects/arrays are stored as raw JSON bytes for round-trip fidelity.
//!
//! This module is the foundation that all executor modules depend on.

const std = @import("std");
const query = @import("kqq_query");

const OrderField = query.OrderField;

// ─── OwnedValue ───────────────────────────────────────────────────────────────

pub const OwnedValue = union(enum) {
    string: []u8, // heap-owned
    number: f64,
    boolean: bool,
    null_val,
    // We deliberately do NOT support nested objects/arrays inside a record
    // for the streaming path — nested values are treated as opaque and skipped
    // unless explicitly navigated (future work).
    // We store them as a raw string for round-trip fidelity.
    raw: []u8, // raw JSON bytes, heap-owned

    pub fn deinit(self: OwnedValue, allocator: std.mem.Allocator) void {
        switch (self) {
            .string => |s| allocator.free(s),
            .raw => |r| allocator.free(r),
            else => {},
        }
    }

    /// Deep-copy an OwnedValue into `allocator`. Strings and raws are duped;
    /// numbers and booleans are copied by value. The caller owns the result.
    pub fn copy(self: OwnedValue, allocator: std.mem.Allocator) !OwnedValue {
        return switch (self) {
            .string => |s| .{ .string = try allocator.dupe(u8, s) },
            .number => |n| .{ .number = n },
            .boolean => |b| .{ .boolean = b },
            .null_val => .null_val,
            .raw => |r| .{ .raw = try allocator.dupe(u8, r) },
        };
    }

    /// Convert to std.json.Value for output. Borrows slices — valid as long as OwnedValue lives.
    pub fn toJsonValue(self: OwnedValue) std.json.Value {
        return switch (self) {
            .string => |s| .{ .string = s },
            .number => |n| .{ .float = n },
            .boolean => |b| .{ .bool = b },
            .null_val => .null,
            .raw => |r| .{ .string = r }, // emit raw as string; good enough for output
        };
    }
};

/// Internal path separator used in the hashmap between path segments.
/// Using ASCII 31 (Unit Separator) instead of '.' so that JSON keys that
/// literally contain dots (e.g. {"svc.metric": 1}) are stored and matched
/// correctly without colliding with nested-object path keys.
pub const PATH_SEP: u8 = '\x1f';

// ─── Record ──────────────────────────────────────────────────────────────────

pub const Record = struct {
    allocator: std.mem.Allocator,
    fields: std.array_hash_map.String(OwnedValue),

    pub fn init(allocator: std.mem.Allocator) Record {
        return .{ .allocator = allocator, .fields = .{} };
    }

    pub fn deinit(self: *Record) void {
        var it = self.fields.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            e.value_ptr.*.deinit(self.allocator);
        }
        self.fields.deinit(self.allocator);
    }

    /// Look up a field by name.  Query field names use '.' as the path separator
    /// (e.g. "address.city") but internal hashmap keys use PATH_SEP ('\x1f').
    /// Strategy:
    ///   1. Try the key as-is — catches top-level keys AND literal dotted keys
    ///      like {"svc.metric": ...} which are stored with their original dots.
    ///   2. If the key contains '.', retry with '.' replaced by PATH_SEP to
    ///      find nested-path keys built by collectObjectInto
    pub fn get(self: *const Record, key: []const u8) ?OwnedValue {
        if (self.fields.get(key)) |v| return v;
        if (std.mem.indexOfScalar(u8, key, '.') != null) {
            var buf: [512]u8 = undefined;
            if (key.len <= buf.len) {
                @memcpy(buf[0..key.len], key);
                for (buf[0..key.len]) |*c| {
                    if (c.* == '.') c.* = PATH_SEP;
                }
                return self.fields.get(buf[0..key.len]);
            }
        }
        return null;
    }
};

// ─── Record copy operations ──────────────────────────────────────────────────

/// Internal: copy a Record, optionally skipping flattened path keys (PATH_SEP).
fn copyRecordImpl(allocator: std.mem.Allocator, src: *const Record, skip_path_keys: bool) !Record {
    var out = Record.init(allocator);
    errdefer out.deinit();
    var it = src.fields.iterator();
    while (it.next()) |e| {
        // When skip_path_keys is true, only copy top-level fields (no PATH_SEP).
        // This is used for output projection where we only want to emit top-level
        // fields, not the flattened "address\x1fcity" query keys.
        if (skip_path_keys and std.mem.indexOfScalar(u8, e.key_ptr.*, PATH_SEP) != null) continue;
        const key = try allocator.dupe(u8, e.key_ptr.*);
        errdefer allocator.free(key);
        const val = try e.value_ptr.*.copy(allocator);
        try out.fields.put(allocator, key, val);
    }
    return out;
}

/// Copy only top-level fields (skip flattened path keys with PATH_SEP).
/// Used for output projection where we only want to emit top-level fields.
pub fn copyRecord(allocator: std.mem.Allocator, src: *const Record) !Record {
    return copyRecordImpl(allocator, src, true);
}

/// Copy ALL keys (including intermediate dotted keys) for internal use where
/// ORDER BY must be able to look up e.g. "address.city" before projection.
pub fn copyRecordFull(allocator: std.mem.Allocator, src: *const Record) !Record {
    return copyRecordImpl(allocator, src, false);
}

// ─── Record comparison (for ORDER BY) ─────────────────────────────────────────

pub fn compareRecords(ob: []const OrderField, a: *const Record, b: *const Record) std.math.Order {
    for (ob) |f| {
        const av = a.get(f.key);
        const bv = b.get(f.key);
        const ord: std.math.Order = blk: {
            if (av != null and bv != null) {
                const a_num = if (av.? == .number) av.?.number else null;
                const b_num = if (bv.? == .number) bv.?.number else null;
                if (a_num != null and b_num != null) {
                    if (a_num.? < b_num.?) break :blk .lt;
                    if (a_num.? > b_num.?) break :blk .gt;
                    break :blk .eq;
                }
                if (av.? == .string and bv.? == .string) {
                    break :blk std.mem.order(u8, av.?.string, bv.?.string);
                }
            }
            break :blk .eq;
        };
        if (ord != .eq) return if (f.dir == .asc) ord else switch (ord) {
            .lt => .gt,
            .gt => .lt,
            .eq => .eq,
        };
    }
    return .eq;
}