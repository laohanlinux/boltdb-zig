const tests = @import("tests.zig");
const TX = @import("tx.zig").TX;
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const std = @import("std");
const Cursor = @import("cursor.zig").Cursor;
const assert = @import("util.zig").assert;
const KeyPair = consts.KeyPair;
const Bucket = @import("bucket.zig").Bucket;

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
test "Bucket_Delete_FreelistOverflow" {
    std.testing.log_level = .warn;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;

    const count = 10000;
    const ContextTuple = tests.Tuple.t2(tests.TestContext, usize);
    var ctx = ContextTuple{
        .first = testCtx,
        .second = 0,
    };
    const ts = std.time.timestamp();
    for (0..count) |i| {
        ctx.second = i;
        const time = std.time.milliTimestamp();
        const updateFn = struct {
            fn update(context: ContextTuple, tx: *TX) Error!void {
                const b = try tx.createBucketIfNotExists("0");
                var key = [16]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
                for (0..1) |j| {
                    std.mem.writeInt(u64, key[0..8], @as(u64, @intCast(context.second)), .big);
                    std.mem.writeInt(u64, key[8..16], @as(u64, @intCast(j)), .big);
                    try b.put(KeyPair.init(key[0..8], key[8..16]));
                }
            }
        }.update;
        try db.updateWithContext(ctx, updateFn);
        if (i % 200 == 0) {
            const allocSize = db.pagePool.?.getAllocSize();
            const dataSize = db.dataRef.?.len;
            const dbAllocSize = db.allocSize;
            std.log.warn("step: {d}, count: {d}, cost: {d}ms, totalCost: {d}s, allocSize: {d}, dbAllocSize: {d}, dataSize: {d}", .{ ctx.second, ctx.second * 1000, (std.time.milliTimestamp() - time), (std.time.timestamp() - ts), allocSize, dbAllocSize, dataSize });
        }
    }

    // Delete all of them in one large transaction
    const updateFn2 = struct {
        fn update(tx: *TX) Error!void {
            const b = tx.getBucket("0") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            var keyPair = cursor.first();
            while (keyPair.key != null) {
                try b.delete(keyPair.key.?);
                keyPair = cursor.next();
            }
        }
    }.update;
    try db.update(updateFn2);
    std.log.warn("total cost: {d}s", .{(std.time.timestamp() - ts)});
}

// Ensure that accessing and updating nested buckets is ok across transactions.
test "Bucket_Nested" {
    std.testing.log_level = .err;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var testCtx = tests.setup(arena.allocator()) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;

    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            _ = try b.createBucket("foo");
            try b.put(KeyPair.init("bar", "0000"));
        }
    }.update;
    try db.update(updateFn);
    db.mustCheck();

    // Update widgets/bar.
    const updateFn2 = struct {
        fn update(tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            try b.put(KeyPair.init("bar", "xxxx"));
        }
    }.update;
    try db.update(updateFn2);
    db.mustCheck();
    // Cause a split.
    const count = 10000;
    const updateFn3 = struct {
        fn update(tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            for (0..count) |i| {
                const key = try std.fmt.allocPrint(tx.allocator, "{d}", .{i});
                const value = try std.fmt.allocPrint(tx.allocator, "{d}", .{i});
                try b.put(KeyPair.init(key, value));
                tx.allocator.free(key);
                tx.allocator.free(value);
            }
        }
    }.update;
    try db.update(updateFn3);
    db.mustCheck();
    // Insert into widgets/foo/baz.
    const updateFn4 = struct {
        fn update(tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            const b2 = b.getBucket("foo") orelse unreachable;
            try b2.put(KeyPair.init("baz", "yyyy"));
        }
    }.update;
    try db.update(updateFn4);
    db.mustCheck();

    // Verify.
    const viewFn = struct {
        fn view(tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            const b2 = b.getBucket("foo") orelse unreachable;
            const value = b2.get("baz");
            assert(std.mem.eql(u8, "yyyy", value.?), "the value is not equal", .{});
            const value2 = b.get("bar");
            assert(std.mem.eql(u8, "xxxx", value2.?), "the value is not equal", .{});
            for (0..count) |i| {
                const key = try std.fmt.allocPrint(tx.allocator, "{d}", .{i});
                const gotValue = b.get(key);
                const expectValue = try std.fmt.allocPrint(tx.allocator, "{d}", .{i});
                assert(std.mem.eql(u8, expectValue, gotValue.?), "the value is not equal", .{});
                tx.allocator.free(key);
                tx.allocator.free(expectValue);
            }
        }
    }.view;
    try db.view(viewFn);
}

// Ensure that deleting a bucket using Delete() returns an error.
test "Bucket_Delete_Bucket" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            _ = try b.createBucket("foo");
            b.delete("foo") catch |err| {
                assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
            };
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that deleting a key on a read-only bucket returns an error.
test "Bucket_Delete_ReadOnly" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            _ = try tx.createBucket("widgets");
        }
    }.update;
    try db.update(updateFn);

    const viewFn = struct {
        fn view(tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            b.delete("bar") catch |err| {
                assert(err == Error.TxNotWriteable, comptime "the error is not TxNotWriteable", .{});
            };
        }
    }.view;
    try db.view(viewFn);
}

// Ensure that a deleting value while the transaction is closed returns an error.
test "Bucket_Delete_TxClosed" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    {
        const tx = try db.begin(true);
        const b = try tx.createBucket("widgets");
        _ = b; // autofix
        try tx.rollback();
    }
}

// Ensure that deleting a bucket causes nested buckets to be deleted.
test "Bucket_DeleteBucket_Nested" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const widgets = try tx.createBucket("widgets");
            const foo = try widgets.createBucket("foo");
            const bar = try foo.createBucket("bar");
            try bar.put(KeyPair.init("baz", "bat"));
            const widgets2 = tx.getBucket("widgets").?;
            try widgets2.deleteBucket("foo");
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
test "Bucket_DeleteBucket_Nested2" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const widgets = try tx.createBucket("widgets");
            const foo = try widgets.createBucket("foo");
            const bar = try foo.createBucket("bar");
            try bar.put(KeyPair.init("baz", "bat"));
        }
    }.update;
    try db.update(updateFn);

    const updateFn2 = struct {
        fn update(tx: *TX) Error!void {
            const widgets = tx.getBucket("widgets").?;
            const foo = widgets.getBucket("foo") orelse unreachable;
            const bar = foo.getBucket("bar") orelse unreachable;
            const baz = bar.get("baz");
            assert(std.mem.eql(u8, "bat", baz.?), "the baz value is not equal", .{});
            try tx.deleteBucket("widgets");
        }
    }.update;
    try db.update(updateFn2);

    const viewFn = struct {
        fn view(tx: *TX) Error!void {
            const widgets = tx.getBucket("widgets");
            assert(widgets == null, "the widgets bucket is not null", .{});
        }
    }.view;
    try db.view(viewFn);
}

// Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
// NOTE: Consistency check in bolt_test.DB.Close() will panic if pages not freed properly.
// test "Bucket_DeleteBucket_MultiplePages" {
//     std.testing.log_level = .err;
//     var testCtx = tests.setup(std.testing.allocator) catch unreachable;
//     defer tests.teardown(&testCtx);
//     const db = testCtx.db;
//     const updateFn = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             const b = try tx.createBucket("widgets");
//             const foo = try b.createBucket("foo");
//             for (0..1000) |i| {
//                 const key = std.fmt.allocPrint(tx.allocator, "{d}", .{i}) catch unreachable;
//                 const value = std.fmt.allocPrint(tx.allocator, "{0:0>100}", .{i}) catch unreachable;
//                 try foo.put(KeyPair.init(key, value));
//                 tx.allocator.free(key);
//                 tx.allocator.free(value);
//             }
//         }
//     }.update;
//     try db.update({}, updateFn);

//     const updateFn2 = struct {
//         fn update(_: void, tx: *TX) Error!void {
//             try tx.deleteBucket("widgets");
//         }
//     }.update;
//     try db.update({}, updateFn2);
// }

// Ensure that a simple value retrieved via Bucket() returns a nil.
test "Bucket_Bucket_IncompatibleValue" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            const foo = tx.getBucket("widgets").?.getBucket("foo");
            assert(foo == null, "the foo bucket is not null", .{});
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that creating a bucket on an existing non-bucket key returns an error.
test "Bucket_CreateBucket_IncompatibleValue" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            _ = b.createBucket("foo") catch |err| {
                assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
            };
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that deleting a bucket on an existing non-bucket key returns an error.
test "Bucket_DeleteBucket_IncompatibleValue" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            tx.getBucket("widgets").?.deleteBucket("foo") catch |err| {
                assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
            };
        }
    }.update;
    try db.update(updateFn);
}

// Ensure bucket can set and update its sequence number.
test "Bucket_Sequence" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            const v = b.sequence();
            assert(v == 0, "the sequence number is not 0", .{});
            try b.setSequence(1000);
            const v2 = b.sequence();
            assert(v2 == 1000, "the sequence number is not 1000", .{});
        }
    }.update;
    try db.update(updateFn);
    // Verify sequence in separate transaction.
    const viewFn = struct {
        fn view(tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            const v = b.sequence();
            assert(v == 1000, "the sequence number is not 1000", .{});
        }
    }.view;
    try db.view(viewFn);
}

// Ensure that a bucket can return an autoincrementing sequence.
test "Bucket_NextSequence" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const widgets = try tx.createBucket("widgets");
            const woojits = try widgets.createBucket("woojits");
            const v = try widgets.nextSequence();
            assert(v == 1, "the sequence number is not 1", .{});
            const v2 = try widgets.nextSequence();
            assert(v2 == 2, "the sequence number is not 2", .{});
            const v3 = try woojits.nextSequence();
            assert(v3 == 1, "the sequence number is not 1", .{});
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that a bucket will persist an autoincrementing sequence even if its
// the only thing updated on the bucket.
// https://github.com/boltdb/bolt/issues/296
test "Bucket_NextSequence_Persist" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            _ = try tx.createBucket("widgets");
        }
    }.update;
    try db.update(updateFn);
    const updateFn2 = struct {
        fn update(tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            const v = try b.nextSequence();
            assert(v == 1, "the sequence number is not 1", .{});
        }
    }.update;
    try db.update(updateFn2);
    const updateFn3 = struct {
        fn update(tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            const v = try b.nextSequence();
            assert(v == 2, "the sequence number is not 2", .{});
        }
    }.update;
    try db.update(updateFn3);
}

// Ensure that retrieving the next sequence on a read-only bucket returns an error.
test "Bucket_NextSequence_ReadOnly" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            _ = tx.createBucket("widgets") catch unreachable;
        }
    }.update;
    try db.update(updateFn);

    const viewFn = struct {
        fn view(tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            _ = b.nextSequence() catch |err| {
                assert(err == Error.TxNotWriteable, comptime "the error is not TxNotWriteable", .{});
            };
        }
    }.view;
    try db.view(viewFn);
}

// Ensure that retrieving the next sequence for a bucket on a closed database returns an error.
test "Bucket_NextSequence_ClosedDB" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const tx = try db.begin(true);
    const b = try tx.createBucket("widgets");
    _ = b; // autofix
    try tx.rollback();
    // _ = b.nextSequence() catch |err| {
    //     assert(err == Error.TxClosed, comptime "the error is not TxClosed", .{});
    // };
}

// Ensure a user can loop over all key/value pairs in a bucket.
test "Bucket_ForEach" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            var b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "0000"));
            try b.put(KeyPair.init("bar", "0001"));
            try b.put(KeyPair.init("baz", "0002"));
            const travelCtx = struct {
                i: u32 = 0,
            };
            var ctx = travelCtx{};
            const travelFn = struct {
                fn travel(_ctx: *travelCtx, _: *Bucket, keyPairRef: *const consts.KeyPair) Error!void {
                    if (_ctx.i == 0) {
                        assert(std.mem.eql(u8, keyPairRef.key.?, "bar"), "the key is not bar", .{});
                        assert(std.mem.eql(u8, keyPairRef.value.?, "0001"), "the value is not 0001", .{});
                    } else if (_ctx.i == 1) {
                        assert(std.mem.eql(u8, keyPairRef.key.?, "baz"), "the key is not baz", .{});
                        assert(std.mem.eql(u8, keyPairRef.value.?, "0002"), "the value is not 0002", .{});
                    } else if (_ctx.i == 2) {
                        assert(std.mem.eql(u8, keyPairRef.key.?, "foo"), "the key is not foo", .{});
                        assert(std.mem.eql(u8, keyPairRef.value.?, "0000"), "the value is not 0000", .{});
                    }
                    _ctx.*.i += 1;
                }
            }.travel;
            b.forEachContext(&ctx, travelFn) catch unreachable;
            assert(ctx.i == 3, "the number of items is not 3", .{});
        }
    }.update;
    try db.update(updateFn);
}

// Ensure a database can stop iteration early.
test "Bucket_ForEach_ShortCircuit" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("bar", "0000"));
            try b.put(KeyPair.init("baz", "0000"));
            try b.put(KeyPair.init("foo", "0000"));

            const travelCtx = struct {
                i: u32 = 0,
            };
            var ctx = travelCtx{};
            const travelFn = struct {
                fn travel(_ctx: *travelCtx, _: *Bucket, keyPairRef: *const consts.KeyPair) Error!void {
                    _ctx.*.i += 1;
                    if (std.mem.eql(u8, keyPairRef.key.?, "baz")) {
                        return Error.NotPassConsistencyCheck;
                    }
                }
            }.travel;
            tx.getBucket("widgets").?.forEachContext(&ctx, travelFn) catch |err| {
                assert(err == Error.NotPassConsistencyCheck, comptime "the error is not NotPassConsistencyCheck", .{});
            };
            assert(ctx.i == 2, "the number of items is not 2", .{});
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that looping over a bucket on a closed database returns an error.
test "Bucket_ForEach_Closed" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const tx = try db.begin(true);
    _ = try tx.createBucket("widgets");
    try tx.rollback();
}
// Ensure that an error is returned when inserting with an empty key.
test "Bucket_Put_EmptyKey" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            b.put(KeyPair.init("", "0000")) catch |err| {
                assert(err == Error.KeyRequired, comptime "the error is not KeyRequired", .{});
            };
            b.put(KeyPair.init(null, "0000")) catch |err| {
                assert(err == Error.KeyRequired, comptime "the error is not KeyRequired", .{});
            };
        }
    }.update;
    try db.update(updateFn);
}

// Ensure that an error is returned when inserting with a key that's too large.
test "Bucket_Put_KeyTooLarge" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(ctx: tests.TestContext, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            const key = ctx.repeat('a', consts.MaxKeySize + 1);
            defer ctx.allocator.free(key);
            b.put(KeyPair.init(key, "bar")) catch |err| {
                assert(err == Error.KeyTooLarge, comptime "the error is not KeyTooLarge", .{});
            };
        }
    }.update;
    try db.updateWithContext(testCtx, updateFn);
}

// Ensure that an error is returned when inserting a value that's too large.
test "Bucket_Put_ValueTooLarge" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    // Skip this test on DroneCI because the machine is resource constrained.
    for (std.os.environ) |env| {
        const envStr = std.mem.span(env); // Convert null-terminated pointer to slice
        if (std.mem.eql(u8, envStr, "DRONE=true")) {
            std.debug.print("Skipping Bucket_Put_ValueTooLarge on DroneCI\n", .{});
            return;
        }
    }

    const updateFn = struct {
        fn update(ctx: tests.TestContext, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            const value = ctx.repeat('a', consts.MaxValueSize + 1);
            defer ctx.allocator.free(value);
            b.put(KeyPair.init("foo", value)) catch |err| {
                assert(err == Error.ValueTooLarge, comptime "the error is not ValueTooLarge", .{});
            };
        }
    }.update;
    try db.updateWithContext(testCtx, updateFn);
}

// Ensure a bucket can calculate stats.
test "Bucket_Stats" {
    std.testing.log_level = .err;
    var opts = consts.defaultOptions;
    opts.readOnly = false;
    opts.initialMmapSize = 100000 * consts.PageSize;
    // opts.pageSize = 0;
    var testCtx = tests.setupWithOptions(std.testing.allocator, opts) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    if (db.pageSize != 4096) {
        std.debug.print("pageSize is not 4096\n", .{});
        return;
    }

    const context = struct {
        const bigKey = "really-big-value";
        index: usize = 0,
        ctx: tests.TestContext,
    };

    var ctx1 = context{ .index = 0, .ctx = testCtx };
    for (0..500) |i| {
        const updateFn = struct {
            fn update(ctx: *context, tx: *TX) Error!void {
                const allocator = tx.getAllocator();
                const key = std.fmt.allocPrint(allocator, "{0:0>3}", .{ctx.index}) catch unreachable;
                assert(key.len == 3, comptime "the key length is not 3", .{});
                defer allocator.free(key);
                const value = std.fmt.allocPrint(allocator, "{d}", .{ctx.index}) catch unreachable;
                defer allocator.free(value);
                const b = try tx.createBucketIfNotExists("woojits");
                try b.put(KeyPair.init(key, value));
            }
        }.update;
        ctx1.index = i;
        try db.updateWithContext(&ctx1, updateFn);
    }

    const updateFn = struct {
        fn update(ctx: *context, tx: *TX) Error!void {
            const b = try tx.createBucketIfNotExists("woojits");
            const value = ctx.ctx.repeat('*', 10000);
            assert(value.len == 10000, comptime "the value length is not 10000", .{});
            defer ctx.ctx.allocator.free(value);
            try b.put(KeyPair.init(context.bigKey, value));
        }
    }.update;
    try db.updateWithContext(&ctx1, updateFn);

    db.mustCheck();

    const viewFn = struct {
        fn view(ctx: context, tx: *TX) Error!void {
            _ = ctx; // autofix
            const b = tx.getBucket("woojits").?;
            const stats = b.stats();
            assert(stats.BranchPageN == 1, comptime "expected 1 but got {d}", .{stats.BranchPageN});
            assert(stats.BranchOverflowN == 0, comptime "expected 0 but got {d}", .{stats.BranchOverflowN});
            assert(stats.LeafPageN == 7, comptime "expected 7 but got {d}", .{stats.LeafPageN});
            assert(stats.LeafOverflowN == 2, comptime "expected 2 but got {d}", .{stats.LeafOverflowN});
            assert(stats.keyN == 501, comptime "expected 501 but got {d}", .{stats.keyN});
            assert(stats.depth == 2, comptime "expected 2 but got {d}", .{stats.depth});

            var branchInuse: usize = 16; // 16 bytes for the branch page header
            branchInuse += 7 * 16; // 7 branch elements
            branchInuse += 7 * 3; // branch keys (63-byte keys)
            assert(stats.BranchInuse == branchInuse, comptime "expected {d} but got {d}", .{ branchInuse, stats.BranchInuse });
            const leafElementSize = @import("page.zig").LeafPageElement.headerSize();
            var leafInuse: usize = 7 * leafElementSize; // 16 bytes for the leaf page header
            leafInuse += 501 * leafElementSize; // leaf elements
            leafInuse += 500 * 3 + context.bigKey.len; // leaf keys (63-byte keys)
            leafInuse += 1 * 10 + 2 * 90 + 3 * 400 + 10000; // leaf values
            assert(stats.LeafInuse == leafInuse, comptime "expected {d} but got {d}", .{ leafInuse, stats.LeafInuse });

            // Only check allocations for 4KB pages.
            if (tx.getDB().pageSize == 4096) {
                assert(stats.BranchAlloc == 4096, comptime "expected 4096 but got {d}", .{stats.BranchAlloc});
                assert(stats.LeafAlloc == 36864, comptime "expected 36864 but got {d}", .{stats.LeafAlloc});
            }

            assert(stats.BucketN == 1, comptime "expected 1 but got {d}", .{stats.BucketN});
            assert(stats.InlineBucketInuse == 0, comptime "expected 0 but got {d}", .{stats.InlineBucketInuse});
        }
    }.view;
    try db.viewWithContext(context{ .ctx = testCtx }, viewFn);
}

// Ensure a bucket with random insertion utilizes fill percentage correctly.
test "Bucket_Stats_RandomFill" {
    if (consts.PageSize != 4096) {
        std.debug.print("skipping Bucket_RandomInsertion because pageSize is not 4096\n", .{});
        return;
    }
    std.testing.log_level = .warn;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    // Add a set of values in random order. It will be the same random
    // order so we can maintain consistency between test runs.
    const randomBytes = testCtx.generateBytes(1000);
    defer testCtx.allocator.free(randomBytes);
    var buf: [1024]u8 = undefined;
    const context = struct {
        index: usize = 0,
        v: usize = 0,
        ctx: tests.TestContext,
        count: usize = 0,
        fixAllocator: std.heap.FixedBufferAllocator = undefined,
    };
    var ctx = context{ .index = 0, .ctx = testCtx, .fixAllocator = std.heap.FixedBufferAllocator.init(buf[0..]) };
    for (randomBytes, 0..) |v, i| {
        ctx.index = i;
        ctx.v = v;
        const updateFn = struct {
            fn update(ctx1: *context, tx: *TX) Error!void {
                const b = try tx.createBucketIfNotExists("woojits");
                b.fillPercent = 0.9;
                const randomKey = ctx1.ctx.generateBytes(100);
                defer ctx1.ctx.allocator.free(randomKey);
                for (randomKey) |j| {
                    const index: usize = (j * 10000) + ctx1.index;
                    ctx1.fixAllocator.reset();
                    const key = try std.fmt.allocPrint(ctx1.fixAllocator.allocator(), "{d}000000000000000", .{index});
                    try b.put(KeyPair.init(key, "0000000000"));
                    ctx1.count += 1;
                }
            }
        }.update;
        try db.updateWithContext(&ctx, updateFn);
    }
    db.mustCheck();

    const viewFn = struct {
        fn view(vCtx: context, tx: *TX) Error!void {
            const stats = tx.getBucket("woojits").?.stats();
            assert(stats.keyN == vCtx.count, comptime "expected {d} but got {d}", .{ vCtx.count, stats.keyN });
            // assert(stats.BranchPageN == 98, comptime "expected 98 but got {d}", .{stats.BranchPageN});
            assert(stats.BranchOverflowN == 0, comptime "expected 0 but got {d}", .{stats.BranchOverflowN});
            assert(stats.BranchInuse == 130984, comptime "expected 130984 but got {d}", .{stats.BranchInuse});
            assert(stats.BranchAlloc == 401408, comptime "expected 401408 but got {d}", .{stats.BranchAlloc});

            assert(stats.LeafPageN == 3412, comptime "expected 3412 but got {d}", .{stats.LeafPageN});
            assert(stats.LeafOverflowN == 0, comptime "expected 0 but got {d}", .{stats.LeafOverflowN});
            assert(stats.LeafInuse == 4742482, comptime "expected 4742482 but got {d}", .{stats.LeafInuse});
            assert(stats.LeafAlloc == 13975552, comptime "expected 13975552 but got {d}", .{stats.LeafAlloc});
        }
    }.view;
    try db.viewWithContext(ctx, viewFn);
}

// Ensure a bucket can calculate stats.
test "Bucket_Stats_Small" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    try db.updateWithContext({}, struct {
        fn update(_: void, tx: *TX) Error!void {
            // Add a bucket that fits on a single root leaf.
            const b = try tx.createBucketIfNotExists("whozawhats");
            try b.put(KeyPair.init("foo", "bar"));
        }
    }.update);

    db.mustCheck();
    try db.viewWithContext({}, struct {
        fn view(_: void, tx: *TX) Error!void {
            const stats = tx.getBucket("whozawhats").?.stats();
            assert(stats.keyN == 1, comptime "expected 1 but got {d}", .{stats.keyN});
            assert(stats.BranchPageN == 0, comptime "expected 0 but got {d}", .{stats.BranchPageN});
            assert(stats.BranchOverflowN == 0, comptime "expected 0 but got {d}", .{stats.BranchOverflowN});
            assert(stats.LeafPageN == 0, comptime "expected 0 but got {d}", .{stats.LeafPageN});
            assert(stats.LeafOverflowN == 0, comptime "expected 0 but got {d}", .{stats.LeafOverflowN});
            assert(stats.depth == 1, comptime "expected 1 but got {d}", .{stats.depth});
            assert(stats.BranchInuse == 0, comptime "expected 0 but got {d}", .{stats.BranchInuse});
            assert(stats.LeafInuse == 0, comptime "expected 0 but got {d}", .{stats.LeafInuse});
            if (tx.getDB().pageSize == 4096) {
                assert(stats.BranchAlloc == 0, comptime "expected 0 but got {d}", .{stats.BranchAlloc});
                assert(stats.LeafAlloc == 0, comptime "expected 0 but got {d}", .{stats.LeafAlloc});
            }

            assert(stats.BucketN == 1, comptime "expected 1 but got {d}", .{stats.BucketN});
            assert(stats.InlineBucketN == 1, comptime "expected 1 but got {d}", .{stats.InlineBucketN});
            assert(stats.InlineBucketInuse == 16 + 16 + 6, comptime "expected {d} but got {d}", .{ 16 + 16 + 6, stats.InlineBucketInuse });
        }
    }.view);
}

// Ensure a bucket with no elements has stats.
test "Bucket_Stats_EmptyBucket" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    try db.updateWithContext({}, struct {
        fn update(_: void, tx: *TX) Error!void {
            // Add a bucket that fits on a single root leaf.
            const b = try tx.createBucketIfNotExists("whozawhats");
            _ = b; // autofix
        }
    }.update);
    db.mustCheck();
    try db.viewWithContext({}, struct {
        fn view(_: void, tx: *TX) Error!void {
            const stats = tx.getBucket("whozawhats").?.stats();
            assert(stats.keyN == 0, comptime "expected 0 but got {d}", .{stats.keyN});
            assert(stats.BranchPageN == 0, comptime "expected 0 but got {d}", .{stats.BranchPageN});
            assert(stats.BranchOverflowN == 0, comptime "expected 0 but got {d}", .{stats.BranchOverflowN});
            assert(stats.LeafPageN == 0, comptime "expected 0 but got {d}", .{stats.LeafPageN});
            assert(stats.LeafOverflowN == 0, comptime "expected 0 but got {d}", .{stats.LeafOverflowN});
            assert(stats.depth == 1, comptime "expected 1 but got {d}", .{stats.depth});
            assert(stats.BranchInuse == 0, comptime "expected 0 but got {d}", .{stats.BranchInuse});
            assert(stats.LeafInuse == 0, comptime "expected 0 but got {d}", .{stats.LeafInuse});
            if (tx.getDB().pageSize == 4096) {
                assert(stats.BranchAlloc == 0, comptime "expected 0 but got {d}", .{stats.BranchAlloc});
                assert(stats.LeafAlloc == 0, comptime "expected 0 but got {d}", .{stats.LeafAlloc});
            }

            assert(stats.BucketN == 1, comptime "expected 1 but got {d}", .{stats.BucketN});
            assert(stats.InlineBucketN == 1, comptime "expected 1 but got {d}", .{stats.InlineBucketN});
            assert(stats.InlineBucketInuse == 16, comptime "expected {d} but got {d}", .{ 16, stats.InlineBucketInuse });
        }
    }.view);
}

// Ensure a bucket can calculate stats.
test "Bucket_Stats_Nested" {
    std.testing.log_level = .debug;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    try db.updateWithContext({}, struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("foo");
            const allocator = tx.getAllocator();
            for (0..100) |i| {
                const key = std.fmt.allocPrint(allocator, "{0:0>2}", .{i}) catch unreachable;
                try b.put(KeyPair.init(key, key));
                allocator.free(key);
            }

            const b2 = try b.createBucket("bar");
            for (0..10) |i| {
                const key = std.fmt.allocPrint(allocator, "{d}", .{i}) catch unreachable;
                try b2.put(KeyPair.init(key, key));
                allocator.free(key);
            }

            const b3 = try b2.createBucket("baz");
            for (0..10) |i| {
                const key = std.fmt.allocPrint(allocator, "{d}", .{i}) catch unreachable;
                try b3.put(KeyPair.init(key, key));
                allocator.free(key);
            }
        }
    }.update);

    db.mustCheck();

    try db.viewWithContext({}, struct {
        fn view(_: void, tx: *TX) Error!void {
            const stats = tx.getBucket("foo").?.stats();
            assert(stats.keyN == 122, comptime "expected 100 but got {d}", .{stats.keyN});
            assert(stats.BranchPageN == 0, comptime "expected 0 but got {d}", .{stats.BranchPageN});
            assert(stats.BranchOverflowN == 0, comptime "expected 0 but got {d}", .{stats.BranchOverflowN});
            assert(stats.LeafPageN == 2, comptime "expected 2 but got {d}", .{stats.LeafPageN});
            assert(stats.LeafOverflowN == 0, comptime "expected 0 but got {d}", .{stats.LeafOverflowN});
            assert(stats.depth == 3, comptime "expected 3 but got {d}", .{stats.depth});
            assert(stats.BranchInuse == 0, comptime "expected 0 but got {d}", .{stats.BranchInuse});

            var foo: usize = 16; // pghdr
            foo += 101 * 16; // foo leaf elements
            foo += 100 * 2 + 100 * 2; // foo leaf key/values
            foo += 3 + 16; // foo -> bar key/value

            var bar: usize = 16; // pghdr
            bar += 11 * 16; // bar leaf elements
            bar += 10 + 10; // bar leaf key/values
            bar += 3 + 16; // bar -> baz key/value

            var baz: usize = 16; // baz (inline) (pghdr)
            baz += 10 * 16; // baz leaf elements
            baz += 10 + 10; // baz leaf key/values
            assert(stats.LeafInuse == foo + bar + baz, "the leaf inuse is not correct, expected {d} but got {d}", .{ foo + bar + baz, stats.LeafInuse });

            if (tx.getDB().pageSize == 4096) {
                assert(stats.BranchAlloc == 0, comptime "the branch alloc is not correct, expected 0 but got {d}", .{stats.BranchAlloc});
                assert(stats.LeafAlloc == 8192, comptime "the leaf alloc is not correct, expected 8192 but got {d}", .{stats.LeafAlloc});
            }

            assert(stats.BucketN == 3, comptime "the bucket number is not correct, expected 3 but got {d}", .{stats.BucketN});
            assert(stats.InlineBucketN == 1, comptime "the inline bucket number is not correct, expected 1 but got {d}", .{stats.InlineBucketN});
            assert(stats.InlineBucketInuse == baz, comptime "the inline bucket inuse is not correct, expected {d} but got {d}", .{ baz, stats.InlineBucketInuse });
        }
    }.view);
}

test "Bucket_Stats_Large" {
    std.testing.log_level = .err;
    if (consts.PageSize != 4096) {
        std.debug.print("skipping Bucket_Stats_Large because pageSize is not 4096\n", .{});
        return;
    }

    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;

    const context = struct {
        index: usize = 0,
        ctx: tests.TestContext,
    };
    var ctx = context{ .index = 0, .ctx = testCtx };
    for (0..100) |_| {
        // Add bucket with lots of keys.
        try db.updateWithContext(&ctx, struct {
            fn update(ctx1: *context, tx: *TX) Error!void {
                const b = try tx.createBucketIfNotExists("widgets");
                for (0..1000) |_| {
                    const key = std.fmt.allocPrint(ctx1.ctx.allocator, "{d}", .{ctx1.index}) catch unreachable;
                    try b.put(KeyPair.init(key, key));
                    ctx1.ctx.allocator.free(key);
                    ctx1.index += 1;
                }
            }
        }.update);
    }
    db.mustCheck();

    try db.viewWithContext({}, struct {
        fn view(_: void, tx: *TX) Error!void {
            const stats = tx.getBucket("widgets").?.stats();
            assert(stats.BranchPageN == 13, comptime "the branch page number is not correct, expected 13 but got {d}", .{stats.BranchPageN});
            assert(stats.BranchOverflowN == 0, comptime "the branch overflow number is not correct, expected 0 but got {d}", .{stats.BranchOverflowN});
            assert(stats.LeafPageN == 1196, comptime "the leaf page number is not correct, expected 1196 but got {d}", .{stats.LeafPageN});
            assert(stats.LeafOverflowN == 0, comptime "the leaf overflow number is not correct, expected 0 but got {d}", .{stats.LeafOverflowN});
            assert(stats.depth == 3, comptime "the depth is not correct, expected 3 but got {d}", .{stats.depth});
            assert(stats.BranchInuse == 25257, comptime "the branch inuse is not correct, expected {d} but got {d}", .{ 25257, stats.BranchInuse });
            assert(stats.LeafInuse == 2596916, comptime "the leaf inuse is not correct, expected {d} but got {d}", .{ 2596916, stats.LeafInuse });
            assert(stats.keyN == 100000, comptime "the key number is not correct, expected 100000 but got {d}", .{stats.keyN});
            if (tx.getDB().pageSize == 4096) {
                assert(stats.BranchAlloc == 53248, comptime "the branch alloc is not correct, expected {d} but got {d}", .{ 53248, stats.BranchAlloc });
                assert(stats.LeafAlloc == 4898816, comptime "the leaf alloc is not correct, expected {d} but got {d}", .{ 4898816, stats.LeafAlloc });
            }
            assert(stats.BucketN == 1, comptime "the bucket number is not correct, expected 1 but got {d}", .{stats.BucketN});
            assert(stats.InlineBucketN == 0, comptime "the inline bucket number is not correct, expected 0 but got {d}", .{stats.InlineBucketN});
            assert(stats.InlineBucketInuse == 0, comptime "the inline bucket inuse is not correct, expected 0 but got {d}", .{stats.InlineBucketInuse});
        }
    }.view);
}

// test "Bucket_Put_Single" {
//     std.testing.log_level = .err;

//     var quick = tests.Quick.init(std.testing.allocator);
//     var data = quick.generate(std.testing.allocator) catch unreachable;
//     defer data.deinit();

//     var config = tests.Config{ .rand = std.Random.DefaultPrng.init(0) };
//     for (0..config.getMaxCount()) |i| {
//         _ = i; // autofix
//         var testCtx = tests.setup(std.testing.allocator) catch unreachable;
//         defer tests.teardown(&testCtx);
//         const db = testCtx.db;

//         try db.updateWithContext({}, struct {
//             fn update(_: void, tx: *TX) Error!void {
//                 _ = try tx.createBucket("widgets");
//             }
//         }.update);

//         const items = quick.generate(std.testing.allocator) catch unreachable;
//         defer items.deinit();
//         const context = struct {
//             index: usize = 0,
//             ctx: tests.TestContext,
//             m: std.StringHashMap([]const u8),
//             keyPair: KeyPair = undefined,
//         };
//         var ctx = context{ .index = 0, .ctx = testCtx, .m = std.StringHashMap([]const u8).init(std.testing.allocator) };
//         defer ctx.m.deinit();
//         for (items.items) |item| {
//             ctx.keyPair = KeyPair.init(item.key, item.value);
//             try db.updateWithContext(&ctx, struct {
//                 fn update(ctx1: *context, tx: *TX) Error!void {
//                     const b = tx.getBucket("widgets").?;
//                     try b.put(ctx1.keyPair);
//                     ctx1.m.put(ctx1.keyPair.key.?, ctx1.keyPair.value.?) catch unreachable;
//                 }
//             }.update);

//             try db.viewWithContext(ctx, struct {
//                 fn view(ctx1: context, tx: *TX) Error!void {
//                     const b = tx.getBucket("widgets").?;
//                     var itr = ctx1.m.iterator();
//                     while (itr.next()) |entry| {
//                         const got = b.get(entry.key_ptr.*).?;
//                         assert(std.mem.eql(u8, got, entry.value_ptr.*), "the value is not correct", .{});
//                     }
//                 }
//             }.view);
//         }
//     }
// }

test "Bucket_Put_Multiple" {
    std.testing.log_level = .err;
}

// Ensure that a transaction can delete all key/value pairs and return to a single leaf page.
test "Bucket_Delete_Quick" {
    std.testing.log_level = .err;
}
