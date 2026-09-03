//! kqq library root – JSON processing utilities.
const std = @import("std");

/// Recursively flatten a JSON value into a single-level object.
/// Nested keys are joined with "." and array indices become e.g. "arr.0".
/// The result is written into `out`, an `std.json.ObjectMap`.
/// `prefix` is the accumulated key path (pass "" at the top level).
pub fn flatten(
    allocator: std.mem.Allocator,
    value: std.json.Value,
    prefix: []const u8,
    out: *std.json.ObjectMap,
) !void {
    switch (value) {
        .object => |obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                const child_key = if (prefix.len == 0)
                    try allocator.dupe(u8, entry.key_ptr.*)
                else
                    try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, entry.key_ptr.* });
                defer allocator.free(child_key);
                try flatten(allocator, entry.value_ptr.*, child_key, out);
            }
        },
        .array => |arr| {
            for (arr.items, 0..) |item, i| {
                const child_key = if (prefix.len == 0)
                    try std.fmt.allocPrint(allocator, "{d}", .{i})
                else
                    try std.fmt.allocPrint(allocator, "{s}.{d}", .{ prefix, i });
                defer allocator.free(child_key);
                try flatten(allocator, item, child_key, out);
            }
        },
        else => {
            // Leaf value — store it. The ObjectMap owns the key copy.
            const owned_key = try allocator.dupe(u8, prefix);
            try out.put(allocator, owned_key, value);
        },
    }
}

test "flatten nested object" {
    const allocator = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        allocator,
        \\{"a":{"b":1},"c":[2,3]}
    ,
        .{},
    );
    defer parsed.deinit();

    var out: std.json.ObjectMap = .{};
    defer {
        var it = out.iterator();
        while (it.next()) |e| allocator.free(e.key_ptr.*);
        out.deinit(allocator);
    }

    try flatten(allocator, parsed.value, "", &out);

    try std.testing.expectEqual(@as(i64, 1), out.get("a.b").?.integer);
    try std.testing.expectEqual(@as(i64, 2), out.get("c.0").?.integer);
    try std.testing.expectEqual(@as(i64, 3), out.get("c.1").?.integer);
}
