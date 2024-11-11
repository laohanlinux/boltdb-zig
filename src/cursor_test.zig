const tests = @import("tests.zig");
const TX = @import("tx.zig").TX;
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const std = @import("std");
const Cursor = @import("cursor.zig").Cursor;
const assert = @import("util.zig").assert;

// Ensure that a cursor can return a reference to the bucket that created it.
test "Cursor_Bucket" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;

    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = trx.createBucket("widgets") catch unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            const cb = cursor.bucket();
            std.debug.assert(@intFromPtr(b) == @intFromPtr(cb));
        }
    }.update;
    try kvDB.update({}, updateFn);
}

test "Cursor_Seek" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(consts.KeyPair.init("foo", "0001"));
            try b.put(consts.KeyPair.init("bar", "0002"));
            try b.put(consts.KeyPair.init("baz", "0003"));
            _ = try b.createBucket("bkt");
        }
    }.update;
    try kvDB.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            // Exact match should go to the key.
            const kv = cursor.seek("bar");
            assert(std.mem.eql(u8, kv.key.?, "bar"), "the key should be 'bar'", .{});
            assert(std.mem.eql(u8, kv.value.?, "0002"), "the value should be '0002'", .{});

            // Inexact match should go to the next key.
            const kv2 = cursor.seek("bas");
            assert(std.mem.eql(u8, kv2.key.?, "baz"), "the key should be 'baz'", .{});
            assert(std.mem.eql(u8, kv2.value.?, "0003"), "the value should be '0003'", .{});

            // Low key should go to the first key.
            const kv3 = cursor.seek("");
            std.debug.assert(std.mem.eql(u8, kv3.key.?, "bar"));
            std.debug.assert(std.mem.eql(u8, kv3.value.?, "0002"));

            // High key should return no key.
            const kv4 = cursor.seek("zzz");
            std.debug.assert(kv4.key == null);
            std.debug.assert(kv4.value == null);

            // Buckets should return their key but no value.
            const kv5 = cursor.seek("bkt");
            std.debug.assert(std.mem.eql(u8, kv5.key.?, "bkt"));
            std.debug.assert(kv5.value == null);
        }
    }.view;
    try kvDB.view({}, viewFn);
}

test "Cursor_Delete" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;

    const count = 1000;
    // Insert every other key between 0 and $count.
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            for (0..count) |i| {
                const key = try std.fmt.allocPrint(std.testing.allocator, "{0:0>10}", .{i});
                defer std.testing.allocator.free(key);
                const value = try std.fmt.allocPrint(std.testing.allocator, "{0:0>10}", .{count + i});
                defer std.testing.allocator.free(value);
                try b.put(consts.KeyPair.init(key, value));
            }
            _ = try b.createBucket("sub");
        }
    }.update;
    try kvDB.update({}, updateFn);

    const updateFn2 = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();

            const key = try std.fmt.allocPrint(std.testing.allocator, "{0:0>10}", .{count / 2});
            defer std.testing.allocator.free(key);

            var keyPair = cursor.first();
            while (!keyPair.isNotFound()) {
                if (std.mem.order(u8, keyPair.key.?, key) == .lt) {
                    try cursor.delete();
                    const got = b.get(keyPair.key.?);
                    assert(got == null, "the key should be deleted, key: {s}", .{keyPair.key.?});
                    keyPair = cursor.next();
                    continue;
                }
                break;
            }
            _ = cursor.seek("sub");
            const err = cursor.delete();
            assert(err == Error.IncompactibleValue, "the error is not bucket not found error, err: {any}", .{err});
        }
    }.update;
    try kvDB.update({}, updateFn2);

    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            const got = b.get("0000000000");
            assert(got == null, "the key should be deleted, key: {s}", .{"0000000000"});
            const stats = b.stats();
            assert(stats.keyN == (count / 2 + 1), "the key number is invalid, keyN: {d}, count: {d}", .{ stats.keyN, count / 2 + 1 });
        }
    }.view;
    try kvDB.view({}, viewFn);
}

// Ensure that a Tx cursor can seek to the appropriate keys when there are a
// large number of keys. This test also checks that seek will always move
// forward to the next key.
//
// Related: https://github.com/boltdb/bolt/pull/187
test "Cursor_Seek_Large" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const count: i64 = 1000;
    const kvDB = testCtx.db;
    // Insert every other key between 0 and $count.
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            var i: i64 = 0;
            while (i < count) : (i += 100) {
                var j: i64 = i;
                while (j < i + 100) : (j += 2) {
                    const key = std.testing.allocator.alloc(u8, 8) catch unreachable;
                    std.mem.writeInt(i64, key[0..8], j, .big);
                    const value = std.testing.allocator.alloc(u8, 100) catch unreachable;
                    try b.put(consts.KeyPair.init(key, value));
                    std.testing.allocator.free(key);
                    std.testing.allocator.free(value);
                }
            }
        }
    }.update;
    try kvDB.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            var keyPair = cursor.first();
            for (0..count) |i| {
                var seek: [8]u8 = undefined;
                const keyNum: i64 = @intCast(i);
                std.mem.writeInt(i64, seek[0..8], keyNum, .big);
                keyPair = cursor.seek(seek[0..]);
                // The last seek is beyond the end of the the range so
                // it should return nil.
                if (i == count - 1) {
                    assert(keyPair.isNotFound(), "the key should be not found, key: {s}", .{seek});
                    continue;
                }
                // Otherwise we should seek to the exact key or the next key.
                const num = std.mem.readInt(i64, keyPair.key.?[0..8], .big);
                if (i % 2 == 0) {
                    assert(num == i, "the key should be seeked to the exact key or the next key, i: {d}, key: {any}, num: {d}", .{ i, seek, num });
                } else {
                    assert(num == i + 1, "the key should be seeked to the next key({d}), i: {d}, key: {any}, num: {d}", .{ i + 1, i, seek, num });
                }
            }
        }
    }.view;
    try kvDB.view({}, viewFn);
}

// Ensure that a cursor can iterate over an empty bucket without error.
test "Cursor_Iterate_EmptyBucket" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.update;
    try kvDB.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            var keyPair = cursor.first();
            assert(keyPair.isNotFound(), "the key should be not found", .{});
        }
    }.view;
    try kvDB.view({}, viewFn);
}

// Ensure that a Tx cursor can reverse iterate over an empty bucket without error.
test "Cursor_EmptyBucketReverse" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.update;
    try kvDB.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            var keyPair = cursor.last();
            assert(keyPair.isNotFound(), "the key should be not found", .{});
        }
    }.view;
    try kvDB.view({}, viewFn);
}

// Ensure that a Tx cursor can iterate over a single root with a couple elements.
test "Cursor_Iterate_Leaf" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(consts.KeyPair.init("baz", ""));
            try b.put(consts.KeyPair.init("foo", &[_]u8{0}));
            try b.put(consts.KeyPair.init("bar", &[_]u8{1}));
        }
    }.update;
    try kvDB.update({}, updateFn);

    const trx = try kvDB.begin(false);
    const bt = trx.getBucket("widgets");
    assert(bt != null, "the bucket should not be null", .{});
    var c = bt.?.cursor();
    const keyPair = c.first();
    assert(std.mem.eql(u8, keyPair.key.?, "bar"), "the key should be 'bar'", .{});
    assert(std.mem.eql(u8, keyPair.value.?, &[_]u8{1}), "the value should be [1]", .{});

    const kv = c.next();
    assert(std.mem.eql(u8, kv.key.?, "baz"), "the key should be 'baz'", .{});
    assert(std.mem.eql(u8, kv.value.?, &[_]u8{}), "the value should be []", .{});

    const kv2 = c.next();
    assert(std.mem.eql(u8, kv2.key.?, "foo"), "the key should be 'foo'", .{});
    assert(std.mem.eql(u8, kv2.value.?, &[_]u8{0}), "the value should be [0]", .{});

    const kv3 = c.next();
    assert(kv3.isNotFound(), "the key should be not found", .{});

    const kv4 = c.next();
    assert(kv4.isNotFound(), "the key should be not found", .{});
    c.deinit();
    try trx.rollback();
}

// Ensure that a cursor can reverse iterate over a single root with a couple elements.
test "Cursor_LeafRootReverse" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(consts.KeyPair.init("baz", ""));
            try b.put(consts.KeyPair.init("foo", &[_]u8{0}));
            try b.put(consts.KeyPair.init("bar", &[_]u8{1}));
        }
    }.update;
    try kvDB.update({}, updateFn);

    const trx = try kvDB.begin(false);
    const bt = trx.getBucket("widgets");
    assert(bt != null, "the bucket should not be null", .{});
    var c = bt.?.cursor();
    const keyPair = c.last();
    assert(std.mem.eql(u8, keyPair.key.?, "foo"), "the key should be 'foo'", .{});
    assert(std.mem.eql(u8, keyPair.value.?, &[_]u8{0}), "the value should be [0]", .{});

    const kv2 = c.prev();
    assert(std.mem.eql(u8, kv2.key.?, "baz"), "the key should be 'baz'", .{});
    assert(std.mem.eql(u8, kv2.value.?, &[_]u8{}), "the value should be []", .{});

    const kv = c.prev();
    assert(std.mem.eql(u8, kv.key.?, "bar"), "the key should be 'bar'", .{});
    assert(std.mem.eql(u8, kv.value.?, &[_]u8{1}), "the value should be [1]", .{});

    const kv3 = c.prev();
    assert(kv3.isNotFound(), "the key should be not found", .{});

    const kv4 = c.prev();
    assert(kv4.isNotFound(), "the key should be not found", .{});
    c.deinit();

    try trx.rollback();
}

// Ensure that a Tx cursor can restart from the beginning.
test "Cursor_Restart" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(consts.KeyPair.init("bar", ""));
            try b.put(consts.KeyPair.init("foo", ""));
        }
    }.update;
    try kvDB.update({}, updateFn);

    const trx = kvDB.begin(false) catch unreachable;
    const bt = trx.getBucket("widgets");
    assert(bt != null, "the bucket should not be null", .{});
    var c = bt.?.cursor();
    const keyPair = c.first();
    assert(std.mem.eql(u8, keyPair.key.?, "bar"), "the key should be 'bar'", .{});

    const keyPair2 = c.next();
    assert(std.mem.eql(u8, keyPair2.key.?, "foo"), "the key should be 'foo'", .{});

    const keyPair3 = c.first();
    assert(std.mem.eql(u8, keyPair3.key.?, "bar"), "the key should be 'bar'", .{});

    const keyPair4 = c.next();
    assert(std.mem.eql(u8, keyPair4.key.?, "foo"), "the key should be 'foo'", .{});

    c.deinit();
    try trx.rollback();
}

// Ensure that a cursor can skip over empty pages that have been deleted.
test "Cursor_First_EmptyPages" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    // Create 1000 keys in the "widgets" bucket.
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
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

// Ensure that a Tx can iterate over all elements in a bucket.
// test "Cursor_QuickCheck" {
//     std.testing.log_level = .err;

//     const f = struct {
//         fn quickCheck(allocator: std.mem.Allocator, size: usize) !void {
//             var q = tests.Quick.init(allocator);
//             q.maxItems = size;
//             // q.maxKeySize = 10;
//             // q.maxValueSize = 10;
//             _ = try q.generate(allocator);
//             defer q.deinit();

//             std.debug.print("QuickCheck passed for size {d}.\n", .{size});
//             var testCtx = try tests.setup(allocator);
//             defer tests.teardown(&testCtx);
//             const kvDB = testCtx.db;
//             {
//                 const trx = try kvDB.begin(true);
//                 const b = try trx.createBucket("widgets");
//                 for (q.items.items) |item| {
//                     try b.put(consts.KeyPair.init(item.key, item.value));
//                 }

//                 try trx.commit();
//                 trx.destroy();
//             }
//             q.sort();

//             // Iterate over all items and check consistency.
//             {
//                 const trx = try kvDB.begin(false);
//                 const b = trx.getBucket("widgets") orelse unreachable;
//                 var cursor = b.cursor();
//                 var keyPair = cursor.first();
//                 for (q.items.items) |item| {
//                     assert(std.mem.eql(u8, keyPair.key.?, item.key), "the key should be {s}", .{item.key});
//                     assert(std.mem.eql(u8, keyPair.value.?, item.value), "the value should be {s}", .{item.value});
//                     keyPair = cursor.next();
//                 }
//                 cursor.deinit();
//                 try trx.rollback();
//             }
//         }
//     }.quickCheck;
//     try f(std.testing.allocator, 500);
// }

// test "Cursor_QuickCheck_Reverse" {
//     // TODO
//     std.testing.log_level = .err;
//     const f = struct {
//         fn quickCheckReverse(allocator: std.mem.Allocator, size: usize) !void {
//             // TODO
//             var q = tests.Quick.init(allocator);
//             q.maxItems = size;
//             _ = try q.generate(allocator);
//             defer q.deinit();
//             var testCtx = try tests.setup(allocator);
//             defer tests.teardown(&testCtx);
//             const kvDB = testCtx.db;
//             // Bulk insert all values.
//             {
//                 const trx = kvDB.begin(true) catch unreachable;
//                 const b = trx.createBucket("widgets") catch unreachable;
//                 for (q.items.items) |item| {
//                     try b.put(consts.KeyPair.init(item.key, item.value));
//                 }
//                 try trx.commitAndDestroy();
//             }

//             // Sort test data.
//             q.reverse();

//             // Iterate over all items and check consistency.
//             {
//                 const trx = kvDB.begin(false) catch unreachable;
//                 const b = trx.getBucket("widgets") orelse unreachable;
//                 var cursor = b.cursor();
//                 var keyPair = cursor.last();
//                 for (q.items.items) |item| {
//                     assert(std.mem.eql(u8, keyPair.key.?, item.key), "the key should be {s}", .{item.key});
//                     keyPair = cursor.prev();
//                 }
//                 cursor.deinit();
//                 try trx.rollback();
//             }
//         }
//     }.quickCheckReverse;
//     try f(std.testing.allocator, 500);
// }

// Ensure that a Tx cursor can iterate over subbuckets.
test "Cursor_QuickCheck_BucketsOnly" {
    // TODO
    std.testing.log_level = .err;
    var db = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&db);
    const kvDB = db.db;
    const bucket_names = [_][]const u8{ "foo", "bar", "baz" };
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            for (bucket_names) |name| {
                _ = try b.createBucket(name);
            }
        }
    }.update;
    try kvDB.update({}, updateFn);

    const expected_bucket_names = [_][]const u8{ "bar", "baz", "foo" };
    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            var keyPair = cursor.first();
            var i: usize = 0;
            while (!keyPair.isNotFound()) {
                assert(std.mem.eql(u8, keyPair.key.?, expected_bucket_names[i]), "the key should be {s}", .{expected_bucket_names[i]});
                assert(keyPair.value == null, "the value should be null", .{});
                keyPair = cursor.next();
                i += 1;
            }
            assert(i == bucket_names.len, "the number of keys should be {d}, but got {d}", .{ bucket_names.len, i });
        }
    }.view;
    try kvDB.view({}, viewFn);
}

// Ensure that a Tx cursor can reverse iterate over subbuckets.
test "Cursor_QuickCheck_BucketsOnly_Reverse" {
    // TODO
    std.testing.log_level = .err;
    var db = tests.setup(std.testing.allocator) catch unreachable;
    defer tests.teardown(&db);
    const kvDB = db.db;
    const bucket_names = [_][]const u8{ "foo", "bar", "baz" };
    const expected_bucket_names = [_][]const u8{ "foo", "baz", "bar" };
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            for (bucket_names) |name| {
                _ = try b.createBucket(name);
            }
        }
    }.update;
    try kvDB.update({}, updateFn);

    const viewFn = struct {
        fn view(_: void, trx: *TX) Error!void {
            // TODO
            const b = trx.getBucket("widgets") orelse unreachable;
            var cursor = b.cursor();
            defer cursor.deinit();
            var keyPair = cursor.last();
            var i: usize = 0;
            while (!keyPair.isNotFound()) {
                assert(std.mem.eql(u8, keyPair.key.?, expected_bucket_names[i]), "the key should be {s}={s}", .{ expected_bucket_names[i], keyPair.key.? });
                keyPair = cursor.prev();
                i += 1;
            }
            assert(i == bucket_names.len, "the number of keys should be {d}, but got {d}", .{ bucket_names.len, i });
        }
    }.view;
    try kvDB.view({}, viewFn);
}

test "ExampleCursor" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    // Start a read-write transaction.
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            // Create a new bucket.
            const b = trx.createBucket("animals") catch unreachable;

            // Insert data into a bucket.
            try b.put(consts.KeyPair.init("dog", "fun"));
            try b.put(consts.KeyPair.init("cat", "lame"));
            try b.put(consts.KeyPair.init("liger", "awesome"));

            // Create a cursor for iteration.
            var c = b.cursor();
            defer c.deinit();

            // Iterate over the bucket.
            var keyPair = c.first();
            while (!keyPair.isNotFound()) {
                std.debug.print("A {s} is {s}.\n", .{ keyPair.key.?, keyPair.value.? });
                // Do something with keyPair.
                keyPair = c.next();
            }
        }
    }.update;
    try kvDB.update({}, updateFn);
}

test "ExampleCursor_reverse" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const updateFn = struct {
        fn update(_: void, trx: *TX) Error!void {
            // Create a new bucket.
            const b = trx.createBucket("animals") catch unreachable;

            // Insert data into a bucket.
            try b.put(consts.KeyPair.init("dog", "fun"));
            try b.put(consts.KeyPair.init("cat", "lame"));
            try b.put(consts.KeyPair.init("liger", "awesome"));

            // Create a cursor for iteration.
            var c = b.cursor();
            defer c.deinit();
            // Iterate over items in reverse sorted key order. This starts
            // from the last key/value pair and updates the k/v variables to
            // the previous key/value on each iteration.
            //
            // The loop finishes at the beginning of the cursor when a nil key
            // is returned.
            var keyPair = c.last();
            while (!keyPair.isNotFound()) {
                std.debug.print("A {s} is {s}.\n", .{ keyPair.key.?, keyPair.value.? });
                keyPair = c.prev();
            }
        }
    }.update;
    try kvDB.update({}, updateFn);
    // Output:
    // A liger is awesome.
    // A dog is fun.
    // A cat is lame.
}
