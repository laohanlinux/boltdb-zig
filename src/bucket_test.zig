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
        try db.update(ctx, updateFn);
        if (i % 200 == 0) {
            const allocSize = db.pagePool.?.getAllocSize();
            const dataSize = db.dataRef.?.len;
            const dbAllocSize = db.allocSize;
            std.log.warn("step: {d}, count: {d}, cost: {d}ms, totalCost: {d}s, allocSize: {d}, dbAllocSize: {d}, dataSize: {d}", .{ ctx.second, ctx.second * 1000, (std.time.milliTimestamp() - time), (std.time.timestamp() - ts), allocSize, dbAllocSize, dataSize });
        }
    }

    // Delete all of them in one large transaction
    const updateFn2 = struct {
        fn update(_: void, tx: *TX) Error!void {
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
    try db.update({}, updateFn2);
    std.log.warn("total cost: {d}s", .{(std.time.timestamp() - ts)});
}

// Ensure that accessing and updating nested buckets is ok across transactions.
test "Bucket_Nested" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;

    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            _ = try b.createBucket("foo");
            try b.put(KeyPair.init("bar", "0000"));
        }
    }.update;
    try db.update({}, updateFn);
    db.mustCheck();

    // Update widgets/bar.
    const updateFn2 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            try b.put(KeyPair.init("bar", "xxxx"));
        }
    }.update;
    try db.update({}, updateFn2);
    db.mustCheck();
    // Cause a split.
    const updateFn3 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            for (0..10000) |i| {
                const key = std.fmt.allocPrint(tx.allocator, "{d}", .{i}) catch unreachable;
                const value = std.fmt.allocPrint(tx.allocator, "{d}", .{i}) catch unreachable;
                try b.put(KeyPair.init(key, value));
                tx.allocator.free(key);
                tx.allocator.free(value);
            }
        }
    }.update;
    try db.update({}, updateFn3);
    db.mustCheck();

    // Insert into widgets/foo/baz.
    const updateFn4 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            const b2 = b.getBucket("foo") orelse unreachable;
            try b2.put(KeyPair.init("baz", "yyyy"));
        }
    }.update;
    try db.update({}, updateFn4);
    db.mustCheck();

    // Verify.
    const viewFn = struct {
        fn view(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            const b2 = b.getBucket("foo") orelse unreachable;
            const value = b2.get("baz");
            assert(std.mem.eql(u8, "yyyy", value.?), "the value is not equal", .{});
            const value2 = b.get("bar");
            assert(std.mem.eql(u8, "xxxx", value2.?), "the value is not equal", .{});
            for (0..10000) |i| {
                const key = std.fmt.allocPrint(tx.allocator, "{d}", .{i}) catch unreachable;
                const gotValue = b.get(key);
                const expectValue = std.fmt.allocPrint(tx.allocator, "{d}", .{i}) catch unreachable;
                assert(std.mem.eql(u8, expectValue, gotValue.?), "the value is not equal", .{});
                tx.allocator.free(key);
                tx.allocator.free(expectValue);
            }
        }
    }.view;
    try db.view({}, viewFn);
}

// Ensure that deleting a bucket using Delete() returns an error.
test "Bucket_Delete_Bucket" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            _ = try b.createBucket("foo");
            b.delete("foo") catch |err| {
                assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
            };
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that deleting a key on a read-only bucket returns an error.
test "Bucket_Delete_ReadOnly" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            _ = try tx.createBucket("widgets");
        }
    }.update;
    try db.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            b.delete("bar") catch |err| {
                assert(err == Error.TxNotWriteable, comptime "the error is not TxNotWriteable", .{});
            };
        }
    }.view;
    try db.view({}, viewFn);
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
        fn update(_: void, tx: *TX) Error!void {
            const widgets = try tx.createBucket("widgets");
            const foo = try widgets.createBucket("foo");
            const bar = try foo.createBucket("bar");
            try bar.put(KeyPair.init("baz", "bat"));
            const widgets2 = tx.getBucket("widgets").?;
            try widgets2.deleteBucket("foo");
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
test "Bucket_DeleteBucket_Nested2" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const widgets = try tx.createBucket("widgets");
            const foo = try widgets.createBucket("foo");
            const bar = try foo.createBucket("bar");
            try bar.put(KeyPair.init("baz", "bat"));
        }
    }.update;
    try db.update({}, updateFn);

    const updateFn2 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const widgets = tx.getBucket("widgets").?;
            const foo = widgets.getBucket("foo") orelse unreachable;
            const bar = foo.getBucket("bar") orelse unreachable;
            const baz = bar.get("baz");
            assert(std.mem.eql(u8, "bat", baz.?), "the baz value is not equal", .{});
            try tx.deleteBucket("widgets");
        }
    }.update;
    try db.update({}, updateFn2);

    const viewFn = struct {
        fn view(_: void, tx: *TX) Error!void {
            const widgets = tx.getBucket("widgets");
            assert(widgets == null, "the widgets bucket is not null", .{});
        }
    }.view;
    try db.view({}, viewFn);
}

// Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
// NOTE: Consistency check in bolt_test.DB.Close() will panic if pages not freed properly.
test "Bucket_DeleteBucket_MultiplePages" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            const foo = try b.createBucket("foo");
            for (0..1000) |i| {
                const key = std.fmt.allocPrint(tx.allocator, "{d}", .{i}) catch unreachable;
                const value = std.fmt.allocPrint(tx.allocator, "{0:0>100}", .{i}) catch unreachable;
                try foo.put(KeyPair.init(key, value));
                tx.allocator.free(key);
                tx.allocator.free(value);
            }
        }
    }.update;
    try db.update({}, updateFn);

    const updateFn2 = struct {
        fn update(_: void, tx: *TX) Error!void {
            try tx.deleteBucket("widgets");
        }
    }.update;
    try db.update({}, updateFn2);
}

// Ensure that a simple value retrieved via Bucket() returns a nil.
test "Bucket_Bucket_IncompatibleValue" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            const foo = tx.getBucket("widgets").?.getBucket("foo");
            assert(foo == null, "the foo bucket is not null", .{});
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that creating a bucket on an existing non-bucket key returns an error.
test "Bucket_CreateBucket_IncompatibleValue" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            _ = b.createBucket("foo") catch |err| {
                assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
            };
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that deleting a bucket on an existing non-bucket key returns an error.
test "Bucket_DeleteBucket_IncompatibleValue" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            tx.getBucket("widgets").?.deleteBucket("foo") catch |err| {
                assert(err == Error.IncompactibleValue, comptime "the error is not IncompatibleValue", .{});
            };
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure bucket can set and update its sequence number.
test "Bucket_Sequence" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            const v = b.sequence();
            assert(v == 0, "the sequence number is not 0", .{});
            try b.setSequence(1000);
            const v2 = b.sequence();
            assert(v2 == 1000, "the sequence number is not 1000", .{});
        }
    }.update;
    try db.update({}, updateFn);
    // Verify sequence in separate transaction.
    const viewFn = struct {
        fn view(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            const v = b.sequence();
            assert(v == 1000, "the sequence number is not 1000", .{});
        }
    }.view;
    try db.view({}, viewFn);
}

// Ensure that a bucket can return an autoincrementing sequence.
test "Bucket_NextSequence" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
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
    try db.update({}, updateFn);
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
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            _ = b; // autofix
        }
    }.update;
    try db.update({}, updateFn);
    const updateFn2 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            const v = try b.nextSequence();
            assert(v == 1, "the sequence number is not 1", .{});
        }
    }.update;
    try db.update({}, updateFn2);
    const updateFn3 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            const v = try b.nextSequence();
            assert(v == 2, "the sequence number is not 2", .{});
        }
    }.update;
    try db.update({}, updateFn3);
}

// Ensure that retrieving the next sequence on a read-only bucket returns an error.
test "Bucket_NextSequence_ReadOnly" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            _ = tx.createBucket("widgets") catch unreachable;
        }
    }.update;
    try db.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets").?;
            _ = b.nextSequence() catch |err| {
                assert(err == Error.TxNotWriteable, comptime "the error is not TxNotWriteable", .{});
            };
        }
    }.view;
    try db.view({}, viewFn);
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
        fn update(_: void, tx: *TX) Error!void {
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
    try db.update({}, updateFn);
}

// Ensure a database can stop iteration early.
test "Bucket_ForEach_ShortCircuit" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
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
    try db.update({}, updateFn);
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
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            b.put(KeyPair.init("", "0000")) catch |err| {
                assert(err == Error.KeyRequired, comptime "the error is not KeyRequired", .{});
            };
            b.put(KeyPair.init(null, "0000")) catch |err| {
                assert(err == Error.KeyRequired, comptime "the error is not KeyRequired", .{});
            };
        }
    }.update;
    try db.update({}, updateFn);
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
    try db.update(testCtx, updateFn);
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
    try db.update(testCtx, updateFn);
}

// Ensure a bucket can calculate stats.
test "Bucket_Stats" {
    std.testing.log_level = .err;
    var testCtx = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&testCtx);
    const db = testCtx.db;
    const bigKey = "really-big-value";
    _ = bigKey; // autofix

    for (0..500) |i| {
        const key = std.fmt.allocPrint(testCtx.allocator, "{0:0>3}", .{i}) catch unreachable;
        const updateFn = struct {
            fn update(buf: []u8, tx: *TX) Error!void {
                const b = try tx.createBucketIfNotExists("widgets");
                try b.put(KeyPair.init(buf, buf));
            }
        }.update;

        try db.update(key, updateFn);
        testCtx.allocator.free(key);
    }

    // const updateFn = struct {
    //     fn update(buf: []u8, tx: *TX) Error!void {
    //         _ = tx; // autofix
    //         _ = buf; // autofix
    //     }
    // }.update;
    // try db.update(testCtx, updateFn);
}
