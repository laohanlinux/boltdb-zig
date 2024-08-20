const std = @import("std");
const testing = std.testing;

export fn add(a: i32, b: i32) i32 {
    return a + b;
}

test "basic add functionality" {
    std.testing.log_level = .debug;
    try testing.expect(add(3, 7) == 10);

    var pending = std.AutoHashMap(u64, std.ArrayList(usize)).init(testing.allocator);
    defer pending.deinit();
    defer {
        var it = pending.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
    }
    for (0..10) |i| {
        const entry = try pending.getOrPut(i);
        if (!entry.found_existing) {
            entry.value_ptr.* = std.ArrayList(usize).init(testing.allocator);
        }
        for (0..10) |j| {
            try entry.value_ptr.append(j);
        }
    }

    var arrayList = std.testing.allocator.create(std.ArrayList(usize)) catch unreachable;
    arrayList.* = std.ArrayList(usize).init(testing.allocator);
    defer std.testing.allocator.destroy(arrayList);
    defer arrayList.deinit();
    for (0..1000) |i| {
        arrayList.append(i) catch unreachable;
        std.log.info("arrayList.items.len: {}", .{arrayList.items.len});
    }
}
