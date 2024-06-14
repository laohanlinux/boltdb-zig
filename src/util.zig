const std = @import("std");

/// Compares two byte slices lexicographically.
pub fn cmpBytes(a: []const u8, b: []const u8) std.math.Order {
    var i: usize = 0;
    var j: usize = 0;
    while (i < a.len and j < b.len) {
        if (a[i] < b[j]) {
            return std.math.Order.lt;
        } else if (a[i] > b[j]) {
            return std.math.Order.gt;
        }
        i += 1;
        j += 1;
    }

    if (i < a.len) {
        return std.math.Order.gt;
    } else if (j < b.len) {
        return std.math.Order.lt;
    } else {
        return std.math.Order.eq;
    }
}

/// Returns true if `a` is less than `b`.
pub fn lessThan(a: []const u8, b: []const u8) bool {
    return cmpBytes(a, b) == std.math.Order.lt;
}

/// Returns true if `a` is equal to `b`.
pub fn equals(a: []const u8, b: []const u8) bool {
    return cmpBytes(a, b) == std.math.Order.eq;
}

/// Returns true if `a` is greater than `b`.
pub fn greaterThan(a: []const u8, b: []const u8) bool {
    return cmpBytes(a, b) == std.math.Order.gt;
}

pub fn assert(ok: bool, comptime fmt: []const u8, args: anytype) void {
    if (ok) {
        return;
    }
    const allocator = std.heap.page_allocator;
    const s = std.fmt.allocPrint(allocator, fmt, args) catch unreachable;
    std.debug.print("{s}\n", .{s});
    defer allocator.free(s);
    @panic(s);
}
