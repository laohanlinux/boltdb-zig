const tests = @import("tests.zig");
const TX = @import("tx.zig").TX;
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const std = @import("std");
const Cursor = @import("cursor.zig").Cursor;
const assert = @import("util.zig").assert;
const KeyPair = consts.KeyPair;

// Ensure that a bucket that gets a non-existent key returns nil.
test "Bucket_Get_NonExistent" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            const keyPair = b.get("foo");
            assert(keyPair == null, comptime "keyPair is not null", .{});
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that a bucket can read a value that is not flushed yet.
test "Bucket_Get_FromNode" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            const value = b.get("foo");
            assert(value != null, comptime "value is null", .{});
            assert(std.mem.eql(u8, value.?, "bar"), comptime "value is not bar", .{});
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that a bucket retrieved via Get() returns a nil.
test "Bucket_Get_IncompatibleValue" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            _ = try b.createBucket("foo");
            const b2 = tx.getBucket("widgets");
            assert(b2 != null, comptime "b2 is not null", .{});
            const b3 = b2.?.get("foo");
            assert(b3 == null, comptime "foo is a subbucket, not a value", .{});
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that a slice returned from a bucket has a capacity equal to its length.
// This also allows slices to be appended to since it will require a realloc by Go.
//
// https://github.com/boltdb/bolt/issues/544
test "Bucket_Get_Capacity" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    // Write key to a bucket.
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("key", "value"));
        }
    }.update;
    try db.update({}, updateFn);

    // Retrieve value and attempt to append to it.
    const updateFn2 = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = tx.getBucket("widgets");
            assert(b != null, comptime "b is not null", .{});
            var cursor = b.?.cursor();
            defer cursor.deinit();
            const keyPair = cursor.first();
            assert(keyPair.key != null, comptime "keyPair is not null", .{});
            assert(std.mem.eql(u8, keyPair.key.?, "key"), comptime "keyPair is not key", .{});
            // TODO: Append to value.
        }
    }.update;
    try db.update({}, updateFn2);
}

// Ensure that a bucket can write a key/value.
test "Bucket_Put" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            const b2 = tx.getBucket("widgets");
            const value = b2.?.get("foo");
            assert(value != null, comptime "value is null", .{});
            assert(std.mem.eql(u8, value.?, "bar"), comptime "value is not bar", .{});
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that a bucket can rewrite a key in the same transaction.
test "Bucket_Put_Repeat" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    const updateFn = struct {
        fn update(_: void, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            try b.put(KeyPair.init("foo", "baz"));
            const b2 = tx.getBucket("widgets");
            const value = b2.?.get("foo");
            assert(value != null, comptime "value is null", .{});
            assert(std.mem.eql(u8, value.?, "baz"), comptime "value is not baz", .{});
        }
    }.update;
    try db.update({}, updateFn);
}

// Ensure that a bucket can write a bunch of large values.
test "Bucket_Put_LargeValues" {
    std.testing.log_level = .err;
    const testCtx = tests.setup() catch unreachable;
    defer tests.teardown(testCtx);
    const db = testCtx.db;
    const count = 100;
    const factor = 200;
    const updateFn = struct {
        fn update(context: tests.TestContext, tx: *TX) Error!void {
            const b = try tx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            var i: usize = 1;
            while (i < count) : (i += 1) {
                const key = context.repeat('0', i * factor);
                const value = context.repeat('X', (count - i) * factor);
                try b.put(KeyPair.init(key, value));
                context.allocator.free(key);
                context.allocator.free(value);
            }
        }
    }.update;
    try db.update(testCtx, updateFn);

    const viewFn = struct {
        fn view(context: tests.TestContext, tx: *TX) Error!void {
            const b = tx.getBucket("widgets") orelse unreachable;
            for (1..count) |i| {
                const key = context.repeat('0', i * factor);
                const value = b.get(key);
                assert(value != null, "the value should be not null", .{});
                const expectValue = context.repeat('X', (count - i) * factor);
                assert(std.mem.eql(u8, expectValue, value.?), "the value is not equal", .{});
                context.allocator.free(key);
                context.allocator.free(expectValue);
            }
        }
    }.view;

    try db.view(testCtx, viewFn);
}
