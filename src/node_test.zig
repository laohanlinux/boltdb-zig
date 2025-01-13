const std = @import("std");
const tests = @import("tests.zig");
const Node = @import("node.zig").Node;
const bucket = @import("bucket.zig");
const tx = @import("tx.zig");
const DB = @import("db.zig");
const consts = @import("consts.zig");
const assert = @import("assert.zig").assert;

// Ensure that a node can insert a key/value.
test "Node_put" {
    std.testing.log_level = .warn;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    const trx = try db.begin(true);
    defer trx.rollbackAndDestroy() catch {};
    const b = try trx.createBucketIfNotExists("test");
    const node = Node.init(testContext.allocator);
    node.bucket = b;
    defer node.deinit();
    // Create mutable buffers for values
    var val2 = [_]u8{'2'};
    var val0 = [_]u8{'0'};
    var val1 = [_]u8{'1'};
    var val3 = [_]u8{'3'};

    _ = node.put("baz", "baz", &val2, 0, 0);
    _ = node.put("foo", "foo", &val0, 0, 0);
    _ = node.put("bar", "bar", &val1, 0, 0);
    _ = node.put("foo", "foo", &val3, 0, consts.intFromFlags(.leaf));
    assert(node.inodes.items.len == 3, "it should have 3 inodes, but got {d}", .{node.inodes.items.len});

    assert(std.mem.eql(u8, node.inodes.items[0].key.?, "bar"), "key should be bar", .{});
    assert(std.mem.eql(u8, node.inodes.items[0].value.?, "1"), "value should be 1", .{});
    assert(std.mem.eql(u8, node.inodes.items[1].key.?, "baz"), "key should be baz", .{});
    assert(std.mem.eql(u8, node.inodes.items[1].value.?, "2"), "value should be 2", .{});
    assert(std.mem.eql(u8, node.inodes.items[2].key.?, "foo"), "key should be foo", .{});
    assert(std.mem.eql(u8, node.inodes.items[2].value.?, "3"), "value should be 3", .{});
    assert(node.inodes.items[2].flags == consts.intFromFlags(.leaf), "flags should be leaf", .{});
}

// Ensure that a node can deserialize from a leaf page.
test "Node_read_LeafPage" {
    const Page = @import("page.zig").Page;
    std.testing.log_level = .warn;

    var buffer = [_]u8{0} ** consts.PageSize;
    @memset(buffer[0..], 0);
    const p = Page.init(&buffer);
    p.count = 2;
    p.flags = consts.intFromFlags(.leaf);
    // Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
    p.leafPageElementPtr(0).* = .{
        .flags = 0,
        .pos = 32,
        .kSize = 3,
        .vSize = 4,
    };
    p.leafPageElementPtr(1).* = .{
        .flags = 0,
        .pos = 23,
        .kSize = 10,
        .vSize = 3,
    };
    // Write data for the nodes at the end.
    var data = p.leafPageElementDataPtr();
    std.mem.copyForwards(u8, data[0..7], "barfooz");
    std.mem.copyForwards(u8, data[7..20], "helloworldbye");

    // Deserialize page into a leaf.
    var node = Node.init(std.testing.allocator);
    defer node.deinit();
    node.read(p);

    // Check that there are two inodes with correct data.
    assert(node.inodes.items.len == 2, "it should have 2 inodes, but got {d}", .{node.inodes.items.len});
    assert(node.isLeaf, "it should be a leaf", .{});
    assert(std.mem.eql(u8, node.inodes.items[0].key.?, "bar"), "key should be bar", .{});
    assert(std.mem.eql(u8, node.inodes.items[0].value.?, "fooz"), "value should be fooz, but got {s}", .{node.inodes.items[0].value.?});
    assert(std.mem.eql(u8, node.inodes.items[1].key.?, "helloworld"), "key should be helloworld", .{});
    assert(std.mem.eql(u8, node.inodes.items[1].value.?, "bye"), "value should be bye", .{});
}

// Ensure that a node can serialize into a leaf page.
test "Node_write_LeafPage" {
    const Page = @import("page.zig").Page;
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    const trx = try db.begin(true);
    defer trx.rollbackAndDestroy() catch {};
    const b = try trx.createBucketIfNotExists("test");
    const node = Node.init(testContext.allocator);
    node.bucket = b;
    node.isLeaf = true;
    defer node.deinit();
    // Create mutable buffers for values
    var val1 = [_]u8{ 'q', 'u', 'e' };
    var val2 = [_]u8{ 'l', 'a', 'k', 'e' };
    var val3 = [_]u8{ 'j', 'o', 'h', 'n', 's', 'o', 'n' };
    _ = node.put("susy", "susy", &val1, 0, 0);
    _ = node.put("ricki", "ricki", &val2, 0, 0);
    _ = node.put("john", "john", &val3, 0, 0);
    // Write it to a page.
    var buffer = [_]u8{0} ** consts.PageSize;
    @memset(buffer[0..], 0);
    const p = Page.init(&buffer);

    _ = node.write(p);

    // Read the page back in.
    const n2 = Node.init(std.testing.allocator);
    defer n2.deinit();
    n2.read(p);
    // Check that the two pages are the same.
    assert(n2.inodes.items.len == 3, "it should have 3 inodes, but got {d}", .{n2.inodes.items.len});
    assert(std.mem.eql(u8, n2.inodes.items[0].key.?, "john"), "key should be john", .{});
    assert(std.mem.eql(u8, n2.inodes.items[0].value.?, "johnson"), "value should be johnson", .{});
    assert(std.mem.eql(u8, n2.inodes.items[1].key.?, "ricki"), "key should be ricki", .{});
    assert(std.mem.eql(u8, n2.inodes.items[1].value.?, "lake"), "value should be lake", .{});
    assert(std.mem.eql(u8, n2.inodes.items[2].key.?, "susy"), "key should be susy", .{});
    assert(std.mem.eql(u8, n2.inodes.items[2].value.?, "que"), "value should be que", .{});
}
