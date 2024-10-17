const tests = @import("tests.zig");
const TX = @import("tx.zig").TX;
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const std = @import("std");
const Cursor = @import("cursor.zig").Cursor;
const assert = @import("util.zig").assert;

// Ensure that a Tx cursor can seek to the appropriate keys when there are a
// large number of keys. This test also checks that seek will always move
// forward to the next key.
//
// Related: https://github.com/boltdb/bolt/pull/187
// test "Cursor_Seek_Large" {
//     const testCtx = tests.setup() catch unreachable;
//     defer tests.teardown(testCtx);
//     const Context = struct {};
//     const count: i64 = 1000;
//     const kvDB = testCtx.db;
//     // Insert every other key between 0 and $count.
//     const updateFn = struct {
//         fn update(ctx: *Context, trx: *TX) Error!void {
//             _ = ctx; // autofix
//             const b = trx.createBucket("widgets") catch unreachable;
//             var i: i64 = 0;
//             while (i < count) : (i += 100) {
//                 var j: i64 = i;
//                 while (j < i + 100) : (j += 2) {
//                     const key = std.testing.allocator.alloc(u8, 8) catch unreachable;
//                     std.mem.writeInt(i64, key[0..8], j, .big);
//                     const value = std.testing.allocator.alloc(u8, 100) catch unreachable;
//                     try b.put(consts.KeyPair.init(key, value));
//                     std.testing.allocator.free(key);
//                     std.testing.allocator.free(value);
//                 }
//             }
//         }
//     }.update;
//     var ctx = Context{};
//     try kvDB.update(&ctx, updateFn);

//     const viewFn = struct {
//         fn view(_: void, trx: *TX) Error!void {
//             const b = trx.getBucket("widgets") orelse unreachable;
//             var cursor = b.cursor();
//             defer cursor.deinit();
//             var keyPair = cursor.first();
//             for (0..count) |i| {
//                 var seek: [8]u8 = undefined;
//                 const keyNum: i64 = @intCast(i);
//                 std.mem.writeInt(i64, seek[0..8], keyNum, .big);
//                 keyPair = cursor.seek(seek[0..]);
//                 // The last seek is beyond the end of the the range so
//                 // it should return nil.
//                 if (i == count - 1) {
//                     assert(keyPair.isNotFound(), "the key should be not found, key: {s}", .{seek});
//                     continue;
//                 }
//                 // Otherwise we should seek to the exact key or the next key.
//                 const num = std.mem.readInt(i64, keyPair.key.?[0..8], .big);
//                 if (i % 2 == 0) {
//                     assert(num == i, "the key should be seeked to the exact key or the next key, i: {d}, key: {any}, num: {d}", .{ i, seek, num });
//                 } else {
//                     assert(num == i + 1, "the key should be seeked to the next key({d}), i: {d}, key: {any}, num: {d}", .{ i + 1, i, seek, num });
//                 }
//             }
//         }
//     }.view;
//     try kvDB.view({}, viewFn);
// }

// Ensure that a cursor can iterate over an empty bucket without error.
// test "Cursor_Iterate_EmptyBucket" {
//     const testCtx = tests.setup() catch unreachable;
//     defer tests.teardown(testCtx);
//     const kvDB = testCtx.db;
//     const Context = struct {};
//     const updateFn = struct {
//         fn update(ctx: *Context, trx: *TX) Error!void {
//             _ = ctx; // autofix
//             const b = trx.createBucket("widgets") catch unreachable;
//             _ = b; // autofix
//         }
//     }.update;
//     var ctx = Context{};
//     try kvDB.update(&ctx, updateFn);

//     const viewFn = struct {
//         fn view(_: void, trx: *TX) Error!void {
//             const b = trx.getBucket("widgets") orelse unreachable;
//             var cursor = b.cursor();
//             defer cursor.deinit();
//             var keyPair = cursor.first();
//             assert(keyPair.isNotFound(), "the key should be not found", .{});
//         }
//     }.view;
//     try kvDB.view({}, viewFn);
// }

// Ensure that a Tx cursor can reverse iterate over an empty bucket without error.
// test "Cursor_EmptyBucketReverse" {
//     const testCtx = tests.setup() catch unreachable;
//     defer tests.teardown(testCtx);
//     const kvDB = testCtx.db;
//     const Context = struct {};
//     const updateFn = struct {
//         fn update(ctx: *Context, trx: *TX) Error!void {
//             _ = ctx; // autofix
//             const b = trx.createBucket("widgets") catch unreachable;
//             _ = b; // autofix
//         }
//     }.update;
//     var ctx = Context{};
//     try kvDB.update(&ctx, updateFn);

//     const viewFn = struct {
//         fn view(_: void, trx: *TX) Error!void {
//             const b = trx.getBucket("widgets") orelse unreachable;
//             var cursor = b.cursor();
//             defer cursor.deinit();
//             var keyPair = cursor.last();
//             assert(keyPair.isNotFound(), "the key should be not found", .{});
//         }
//     }.view;
//     try kvDB.view({}, viewFn);
// }

// Ensure that a Tx cursor can iterate over a single root with a couple elements.
// test "Cursor_Iterate_Leaf" {
//     const testCtx = tests.setup() catch unreachable;
//     defer tests.teardown(testCtx);
//     const kvDB = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, trx: *TX) Error!void {
//             const b = trx.createBucket("widgets") catch unreachable;
//             try b.put(consts.KeyPair.init("baz", ""));
//             try b.put(consts.KeyPair.init("foo", &[_]u8{0}));
//             try b.put(consts.KeyPair.init("bar", &[_]u8{1}));
//         }
//     }.update;
//     try kvDB.update({}, updateFn);

//     const trx = kvDB.begin(false) catch unreachable;
//     const bt = trx.getBucket("widgets");
//     assert(bt != null, "the bucket should not be null", .{});
//     var c = bt.?.cursor();
//     defer c.deinit();
//     const keyPair = c.first();
//     assert(std.mem.eql(u8, keyPair.key.?, "bar"), "the key should be 'bar'", .{});
//     assert(std.mem.eql(u8, keyPair.value.?, &[_]u8{1}), "the value should be [1]", .{});

//     const kv = c.next();
//     assert(std.mem.eql(u8, kv.key.?, "baz"), "the key should be 'baz'", .{});
//     assert(std.mem.eql(u8, kv.value.?, &[_]u8{}), "the value should be []", .{});

//     const kv2 = c.next();
//     assert(std.mem.eql(u8, kv2.key.?, "foo"), "the key should be 'foo'", .{});
//     assert(std.mem.eql(u8, kv2.value.?, &[_]u8{0}), "the value should be [0]", .{});

//     const kv3 = c.next();
//     assert(kv3.isNotFound(), "the key should be not found", .{});

//     const kv4 = c.next();
//     assert(kv4.isNotFound(), "the key should be not found", .{});

//     try trx.rollback();
// }

// Ensure that a cursor can reverse iterate over a single root with a couple elements.
// test "Cursor_LeafRootReverse" {
//     const testCtx = tests.setup() catch unreachable;
//     defer tests.teardown(testCtx);
//     const kvDB = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, trx: *TX) Error!void {
//             const b = trx.createBucket("widgets") catch unreachable;
//             try b.put(consts.KeyPair.init("baz", ""));
//             try b.put(consts.KeyPair.init("foo", &[_]u8{0}));
//             try b.put(consts.KeyPair.init("bar", &[_]u8{1}));
//         }
//     }.update;
//     try kvDB.update({}, updateFn);

//     const trx = kvDB.begin(false) catch unreachable;
//     const bt = trx.getBucket("widgets");
//     assert(bt != null, "the bucket should not be null", .{});
//     var c = bt.?.cursor();
//     defer c.deinit();
//     const keyPair = c.last();
//     assert(std.mem.eql(u8, keyPair.key.?, "foo"), "the key should be 'foo'", .{});
//     assert(std.mem.eql(u8, keyPair.value.?, &[_]u8{0}), "the value should be [0]", .{});

//     const kv2 = c.prev();
//     assert(std.mem.eql(u8, kv2.key.?, "baz"), "the key should be 'baz'", .{});
//     assert(std.mem.eql(u8, kv2.value.?, &[_]u8{}), "the value should be []", .{});

//     const kv = c.prev();
//     assert(std.mem.eql(u8, kv.key.?, "bar"), "the key should be 'bar'", .{});
//     assert(std.mem.eql(u8, kv.value.?, &[_]u8{1}), "the value should be [1]", .{});

//     const kv3 = c.prev();
//     assert(kv3.isNotFound(), "the key should be not found", .{});

//     const kv4 = c.prev();
//     assert(kv4.isNotFound(), "the key should be not found", .{});

//     try trx.rollback();
// }

// Ensure that a Tx cursor can restart from the beginning.
// test "Cursor_Restart" {
//     const testCtx = tests.setup() catch unreachable;
//     defer tests.teardown(testCtx);
//     const kvDB = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, trx: *TX) Error!void {
//             const b = trx.createBucket("widgets") catch unreachable;
//             try b.put(consts.KeyPair.init("bar", ""));
//             try b.put(consts.KeyPair.init("foo", ""));
//         }
//     }.update;
//     try kvDB.update({}, updateFn);

//     const trx = kvDB.begin(false) catch unreachable;
//     const bt = trx.getBucket("widgets");
//     assert(bt != null, "the bucket should not be null", .{});
//     var c = bt.?.cursor();
//     defer c.deinit();
//     const keyPair = c.first();
//     assert(std.mem.eql(u8, keyPair.key.?, "bar"), "the key should be 'bar'", .{});

//     const keyPair2 = c.next();
//     assert(std.mem.eql(u8, keyPair2.key.?, "foo"), "the key should be 'foo'", .{});

//     const keyPair3 = c.first();
//     assert(std.mem.eql(u8, keyPair3.key.?, "bar"), "the key should be 'bar'", .{});

//     const keyPair4 = c.next();
//     assert(std.mem.eql(u8, keyPair4.key.?, "foo"), "the key should be 'foo'", .{});

//     try trx.rollback();
// }

// Ensure that a cursor can skip over empty pages that have been deleted.
test "Cursor_First_EmptyPages" {
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const kvDB = testCtx.db;
    // Create 1000 keys in the "widgets" bucket.
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = trx.createBucket("widgets") catch unreachable;
            var key: [8]u8 = undefined;
            for (0..1000) |i| {
                const keyNum: i64 = @intCast(i);
                std.mem.writeInt(i64, key[0..8], keyNum, .big);
                try b.put(consts.KeyPair.init(key[0..8], ""));
                @memset(key[0..8], 0);
            }
        }
    }.update;
    try kvDB.update({}, updateFn);

    // Delete half the keys and then try to iterate.
    const updateFn2 = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var key: [8]u8 = undefined;
            for (0..600) |i| {
                const keyNum: i64 = @intCast(i);
                std.mem.writeInt(i64, key[0..8], keyNum, .big);
                try b.delete(key[0..8]);
                @memset(key[0..8], 0);
            }
            var c = b.cursor();
            defer c.deinit();
            var n: usize = 0;
            var keyPair = c.first();
            while (!keyPair.isNotFound()) {
                keyPair = c.next();
                n += 1;
            }
            assert(n == 400, "the number of keys should be 400, but got {d}", .{n});
        }
    }.update;
    try kvDB.update({}, updateFn2);
}
