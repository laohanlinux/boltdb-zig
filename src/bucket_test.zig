const tests = @import("tests.zig");
const TX = @import("tx.zig").TX;
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const std = @import("std");
const Cursor = @import("cursor.zig").Cursor;
const assert = @import("util.zig").assert;
const KeyPair = consts.KeyPair;

// Ensure that a bucket that gets a non-existent key returns nil.
// test "Bucket_Get_NonExistent" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             const keyPair = b.get("foo");
//             assert(keyPair == null, comptime "keyPair is not null", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that a bucket can read a value that is not flushed yet.
// test "Bucket_Get_FromNode" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             try b.put(KeyPair.init("foo", "bar"));
//             const value = b.get("foo");
//             assert(value != null, comptime "value is null", .{});
//             assert(std.mem.eql(u8, value.?, "bar"), comptime "value is not bar", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that a bucket retrieved via Get() returns a nil.
// test "Bucket_Get_IncompatibleValue" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             _ = try b.createBucket("foo");
//             const b2 = tx.getBucket("widgets");
//             assert(b2 != null, comptime "b2 is not null", .{});
//             const b3 = b2.?.get("foo");
//             assert(b3 == null, comptime "foo is a subbucket, not a value", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that a slice returned from a bucket has a capacity equal to its length.
// // This also allows slices to be appended to since it will require a realloc by Go.
// //
// // https://github.com/boltdb/bolt/issues/544
// test "Bucket_Get_Capacity" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     // Write key to a bucket.
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             try b.put(KeyPair.init("key", "value"));
//         }
//     }.update;
//     try db.update({}, updateFn);

//     // Retrieve value and attempt to append to it.
//     const updateFn2 = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = tx.getBucket("widgets");
//             assert(b != null, comptime "b is not null", .{});
//             var cursor = b.?.cursor();
//             defer cursor.deinit();
//             const keyPair = cursor.first();
//             assert(keyPair.key != null, comptime "keyPair is not null", .{});
//             assert(std.mem.eql(u8, keyPair.key.?, "key"), comptime "keyPair is not key", .{});
//             // TODO: Append to value.
//         }
//     }.update;
//     try db.update({}, updateFn2);
// }

// // Ensure that a bucket can write a key/value.
// test "Bucket_Put" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             try b.put(KeyPair.init("foo", "bar"));
//             const b2 = tx.getBucket("widgets");
//             const value = b2.?.get("foo");
//             assert(value != null, comptime "value is null", .{});
//             assert(std.mem.eql(u8, value.?, "bar"), comptime "value is not bar", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that a bucket can rewrite a key in the same transaction.
// test "Bucket_Put_Repeat" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             try b.put(KeyPair.init("foo", "bar"));
//             try b.put(KeyPair.init("foo", "baz"));
//             const b2 = tx.getBucket("widgets");
//             const value = b2.?.get("foo");
//             assert(value != null, comptime "value is null", .{});
//             assert(std.mem.eql(u8, value.?, "baz"), comptime "value is not baz", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that a bucket can write a bunch of large values.
// test "Bucket_Put_LargeValue" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const count = 100;
//     const factor = 200;
//     const updateFn = struct {
//         fn update(context: tests.TestContext, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             try b.put(KeyPair.init("foo", "bar"));
//             var i: usize = 1;
//             while (i < count) : (i += 1) {
//                 const key = context.repeat('0', i * factor);
//                 const value = context.repeat('X', (count - i) * factor);
//                 try b.put(KeyPair.init(key, value));
//                 context.allocator.free(key);
//                 context.allocator.free(value);
//             }
//         }
//     }.update;
//     try db.update(testCtx, updateFn);

//     const viewFn = struct {
//         fn view(context: tests.TestContext, tx: *TX) Error!void {
//             const b = tx.getBucket("widgets") orelse unreachable;
//             for (1..count) |i| {
//                 const key = context.repeat('0', i * factor);
//                 const value = b.get(key);
//                 assert(value != null, "the value should be not null", .{});
//                 const expectValue = context.repeat('X', (count - i) * factor);
//                 assert(std.mem.eql(u8, expectValue, value.?), "the value is not equal", .{});
//                 context.allocator.free(key);
//                 context.allocator.free(expectValue);
//             }
//         }
//     }.view;

//     try db.view(testCtx, viewFn);
// }

// // Ensure that a database can perform multiple large appends safely.
// // test "Bucket_Put_VeryLarge" {
// //     std.testing.log_level = .err;
// //     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
// //     defer arenaAllocator.deinit();
// //     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
// //     defer tests.teardown(&testCtx);
// //     const db = testCtx.db;
// //     const n = 400000;
// //     const batchN = 200000;
// //     const vSize: usize = 500;
// //     const ContextTuple = tests.Tuple.t2(tests.TestContext, usize);
// //     var ctx = ContextTuple{
// //         .first = testCtx,
// //         .second = 0,
// //     };

// //     for (0..n) |i| {
// //         ctx.second = i;
// //         const updateFn = struct {
// //             fn update(context: ContextTuple, tx: *TX) Error!void {
// //                 const b = tx.createBucketIfNotExists("widgets") catch unreachable;
// //                 const value = context.first.repeat('A', vSize);
// //                 var key = [4]u8{ 0, 0, 0, 0 };
// //                 for (0..batchN) |j| {
// //                     const keyNum = @as(u32, @intCast(context.second + j));
// //                     std.mem.writeInt(u32, key[0..4], keyNum, .big);
// //                     try b.put(KeyPair.init(key[0..], value));
// //                     if (j % 500 == 0) {
// //                         std.log.err("step: {}: {}", .{ context.second, j });
// //                     }
// //                 }
// //                 context.first.allocator.free(value);
// //             }
// //         }.update;
// //         try db.update(ctx, updateFn);
// //     }
// // }

// // Ensure that a setting a value on a key with a bucket value returns an error.
// test "Bucket_Put_IncompatibleValue" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             _ = try b.createBucket("foo");
//             const err = b.put(KeyPair.init("foo", "bar"));
//             assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that a setting a value while the transaction is closed returns an error.
// test "Bucket_Put_TxClosed" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const tx = try db.begin(true);
//     const b = try tx.createBucket("widgets");
//     _ = b; // autofix
//     try tx.rollback();
//     // const b2 = tx.createBucket("widgets");
//     // assert(b2 == Error.TxClosed, comptime "the error is not TxClosed", .{});
// }

// // Ensure that setting a value on a read-only bucket returns an error.
// test "Bucket_Put_ReadOnly" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             _ = b; // autofix
//         }
//     }.update;
//     try db.update({}, updateFn);

//     const viewFn = struct {
//         fn view(_: void, tx: *TX) Error!void {
//             const b = tx.getBucket("widgets") orelse unreachable;
//             const err = b.put(KeyPair.init("foo", "bar"));
//             assert(err == Error.TxNotWriteable, comptime "the error is not TxNotWriteable", .{});
//         }
//     }.view;
//     try db.view({}, viewFn);
// }

// // Ensure that a bucket can delete an existing key.
// test "Bucket_Delete" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             try b.put(KeyPair.init("foo", "bar"));
//             try b.delete("foo");
//             const value = b.get("foo");
//             assert(value == null, comptime "the value is not null", .{});
//         }
//     }.update;
//     try db.update({}, updateFn);
// }

// // Ensure that deleting a large set of keys will work correctly.
// test "Bucket_Delete_Large" {
//     std.testing.log_level = .err;
//     var arenaAllocator = std.heap.ArenaAllocator.init(std.testing.allocator);
//     defer arenaAllocator.deinit();
//     var testCtx = tests.setup(arenaAllocator.allocator()) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;

//     const updateFn = struct {
//         fn update(context: tests.TestContext, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             const value = context.repeat('X', 1024);
//             var key = [4]u8{ 0, 0, 0, 0 };
//             for (0..100) |i| {
//                 std.mem.writeInt(u32, key[0..4], @as(u32, @intCast(i)), .big);
//                 try b.put(KeyPair.init(key[0..], value));
//             }
//             context.allocator.free(value);
//         }
//     }.update;
//     try db.update(testCtx, updateFn);

//     const updateFn2 = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = tx.getBucket("widgets") orelse unreachable;
//             var key = [4]u8{ 0, 0, 0, 0 };
//             for (0..100) |i| {
//                 std.mem.writeInt(u32, key[0..4], @as(u32, @intCast(i)), .big);
//                 std.log.debug("delete key: {any}", .{key[0..]});
//                 try b.delete(key[0..]);
//             }
//         }
//     }.update;
//     try db.update({}, updateFn2);

//     const viewFn = struct {
//         fn view(_: void, tx: *TX) Error!void {
//             const b = tx.getBucket("widgets") orelse unreachable;
//             var key = [4]u8{ 0, 0, 0, 0 };
//             for (0..100) |i| {
//                 std.log.debug("view key: {any}", .{key[0..]});
//                 std.mem.writeInt(u32, key[0..4], @as(u32, @intCast(i)), .big);
//                 const value = b.get(key[0..]);
//                 assert(value == null, comptime "key: {any}, the value is not null", .{key[0..]});
//             }
//         }
//     }.view;
//     try db.view({}, viewFn);
// }

// Deleting a very large list of keys will cause the freelist to use overflow.
test "Bucket_Delete_Large_Overflow" {
    std.testing.log_level = .debug;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;

    // const viewFn = struct {
    //     fn view(_: void, tx: *TX) Error!void {
    //         _ = tx; // autofix
    //     }
    // }.view;
    // db.view({}, viewFn) catch unreachable;

    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            _ = tx; // autofix
        }
    }.update;
    db.update({}, updateFn) catch unreachable;

    // const count = 1;
    // const ContextTuple = tests.Tuple.t2(tests.TestContext, usize);
    // var ctx = ContextTuple{
    //     .first = testCtx,
    //     .second = 0,
    // };
    // const ts = std.time.timestamp();
    // _ = ts; // autofix

    // for (0..count) |i| {
    //     ctx.second = i;
    //     const time = std.time.milliTimestamp();
    //     _ = time; // autofix
    //     const updateFn = struct {
    //         fn update(context: ContextTuple, tx: *TX) Error!void {
    //             const b = try tx.createBucketIfNotExists("widgets");
    //             var key = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    //             var value = [0]u8{};
    //             for (0..1) |j| {
    //                 std.mem.writeInt(u64, key[0..8], @as(u64, @intCast(context.second)), .big);
    //                 std.mem.writeInt(u64, key[8..16], @as(u64, @intCast(j)), .big);
    //                 try b.put(KeyPair.init(key[0..], value[0..]));
    //             }
    //             // std.log.warn("allocSize: {d}", .{tx.autoFreeNodes.getAllocSize()});
    //         }
    //     }.update;
    //     try db.update(ctx, updateFn);
    //     // _ = arenaAllocator.reset(.free_all);
    //     // if (i % 200 == 0) {
    //     //     const allocSize = db.pagePool.?.getAllocSize();
    //     //     const dataSize = db.dataRef.?.len;
    //     //     const dbAllocSize = db.allocSize;
    //     //     std.log.warn("step: {d}, count: {d}, cost: {d}ms, totalCost: {d}s, allocSize: {d}, dbAllocSize: {d}, dataSize: {d}", .{ ctx.second, ctx.second * 1000, (std.time.milliTimestamp() - time), (std.time.timestamp() - ts), allocSize, dbAllocSize, dataSize });
    //     // }
    // }

    // std.log.warn("total cost: {d}s", .{(std.time.timestamp() - ts)});
}
