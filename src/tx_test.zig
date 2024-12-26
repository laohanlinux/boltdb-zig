const std = @import("std");
const tx = @import("tx.zig");
const tests = @import("tests.zig");
const Error = @import("error.zig").Error;
const assert = @import("util.zig").assert;
const consts = @import("consts.zig");
const PageSize = consts.PageSize;
const defaultOptions = consts.defaultOptions;
const KeyPair = consts.KeyPair;

// Ensure that committing a closed transaction returns an error.
test "Tx_Commit_ErrTxClosed" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    {
        const trx = try kvDB.begin(true);
        defer trx.destroy();
        _ = try trx.createBucket("foo");

        try trx.commit();
        trx.commit() catch |err| {
            try std.testing.expect(err == Error.TxClosed);
        };
    }
    {
        const trx = try kvDB.begin(true);
        _ = try trx.createBucketIfNotExists("foo");
        try trx.commitAndDestroy();
    }
    {
        const trx = try kvDB.begin(true);
        defer trx.destroy();
        _ = try trx.createBucketIfNotExists("foo");
        try trx.commit();
    }
}

// Ensure that rolling back a closed transaction returns an error.
test "Tx_Rollback_ErrTxClosed" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    {
        const trx = try kvDB.begin(true);
        defer trx.destroy();
        try trx.rollback();
        trx.rollback() catch |err| {
            try std.testing.expect(err == Error.TxClosed);
        };
    }

    {
        const trx = try kvDB.begin(true);
        try trx.rollbackAndDestroy();
    }
}

// Ensure that committing a read-only transaction returns an error.
test "Tx_Commit_ErrTxNotWritable" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const trx = try kvDB.begin(false);
    defer trx.rollbackAndDestroy() catch unreachable;
    trx.commit() catch |err| {
        try std.testing.expect(err == Error.TxNotWriteable);
    };
}

// Ensure that a transaction can retrieve a cursor on the root bucket.
test "Tx_Cursor" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucket("widgets");
            _ = try trx.createBucket("woojits");
            var cursor = trx.cursor();
            defer cursor.deinit();
            var keyPair = cursor.first();
            assert(std.mem.eql(u8, keyPair.key.?, "widgets"), "expected key 'widgets'", .{});
            keyPair = cursor.next();
            assert(std.mem.eql(u8, keyPair.key.?, "woojits"), "expected key 'woojits'", .{});
            keyPair = cursor.next();
            assert(keyPair.key == null, "expected nil key", .{});
        }
    }.exec);
}

// Ensure that creating a bucket with a read-only transaction returns an error.
test "Tx_CreateBucket_ErrTxNotWritable" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = trx.createBucket("widgets") catch |err| {
                assert(err == Error.TxNotWriteable, "expected error TxNotWriteable", .{});
            };
        }
    }.exec);
}

// Ensure that creating a bucket on a closed transaction returns an error.
test "Tx_CreateBucket_ErrTxClosed" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const trx = try kvDB.begin(true);
    defer trx.destroy();
    try trx.commit();
    _ = trx.createBucket("widgets") catch |err| {
        assert(err == Error.TxClosed, "expected error TxClosed", .{});
    };
}

// Ensure that a Tx can retrieve a bucket.
test "Tx_Bucket" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucket("widgets");
            const bucket = trx.getBucket("widgets");
            assert(bucket != null, "expected bucket 'widgets'", .{});
        }
    }.exec);
}

// Ensure that a Tx retrieving a non-existent key returns nil.
test "Tx_Get_NotFound" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const bt = try trx.createBucket("widgets");
            try bt.put(KeyPair.init("foo", "bar"));
            const notFound = bt.get("no_such_key");
            assert(notFound == null, "expected nil value", .{});
        }
    }.exec);
}

// Ensure that a bucket can be created and retrieved.
test "Tx_CreateBucket" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.exec);
    // Read the bucket through a separate transaction.
    try kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            const bucket = trx.getBucket("widgets");
            assert(bucket != null, "expected bucket 'widgets'", .{});
        }
    }.exec);
}

// Ensure that a bucket can be created if it doesn't already exist.
test "Tx_CreateBucketIfNotExists" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucketIfNotExists("widgets");
            _ = try trx.createBucketIfNotExists("widgets");
        }
    }.exec);

    // Read the bucket through a separate transaction.
    try kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            const bucket = trx.getBucket("widgets");
            assert(bucket != null, "expected bucket 'widgets'", .{});
        }
    }.exec);
}

// Ensure transaction returns an error if creating an unnamed bucket.
test "Tx_CreateBucketIfNotExists_ErrBucketNameRequired" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = trx.createBucketIfNotExists("") catch |err| {
                assert(err == Error.BucketNameRequired, "expected error BucketNameRequired", .{});
            };
        }
    }.exec);
}

// Ensure that a bucket cannot be created twice.
test "Tx_CreateBucket_ErrBucketExists" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.exec);
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = trx.createBucket("widgets") catch |err| {
                assert(err == Error.BucketExists, "expected error BucketExists", .{});
            };
        }
    }.exec);
}

// Ensure that a bucket is created with a non-blank name.
test "Tx_CreateBucket_ErrBucketNameRequired" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = trx.createBucket("") catch |err| {
                assert(err == Error.BucketNameRequired, "expected error BucketNameRequired", .{});
            };
        }
    }.exec);
}

// Ensure that a bucket can be deleted.
test "Tx_DeleteBucket" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
        }
    }.exec);

    // Delete the bucket and make sure we can't get the value.
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            try trx.deleteBucket("widgets");
            const bt = trx.getBucket("widgets");
            assert(bt == null, "expected nil bucket", .{});
        }
    }.exec);

    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            // Create the bucket again and make sure there's not a phantom value.
            const bt = try trx.createBucket("widgets");
            const value = bt.get("foo");
            assert(value == null, "expected nil value", .{});
        }
    }.exec);
}

// Ensure that deleting a bucket on a closed transaction returns an error.
test "Tx_DeleteBucket_ErrTxClosed" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const trx = try kvDB.begin(true);
    defer trx.destroy();
    try trx.commit();
    trx.deleteBucket("widgets") catch |err| {
        assert(err == Error.TxClosed, "expected error TxClosed", .{});
    };
}

// Ensure that deleting a bucket with a read-only transaction returns an error.
test "Tx_DeleteBucket_ErrTxNotWritable" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            trx.deleteBucket("widgets") catch |err| {
                assert(err == Error.TxNotWriteable, "expected error TxNotWriteable", .{});
            };
        }
    }.exec);
}

// Ensure that nothing happens when deleting a bucket that doesn't exist.
test "Tx_DeleteBucket_NotFound" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    _ = try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            trx.deleteBucket("widgets") catch |err| {
                assert(err == Error.BucketNotFound, "expected error BucketNotFound", .{});
            };
        }
    }.exec);
}

// Ensure that no error is returned when a tx.ForEach function does not return
// an error.
test "Tx_ForEach_NoError" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const bt = try trx.createBucket("widgets");
            try bt.put(KeyPair.init("foo", "bar"));
            try trx.forEach(struct {
                fn f(_: []const u8) Error!void {}
            }.f);
        }
    }.exec);
}

// Ensure that an error is returned when a tx.ForEach function returns an error.
test "Tx_ForEach_WithError" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const bt = try trx.createBucket("widgets");
            try bt.put(KeyPair.init("foo", "bar"));
            trx.forEach(struct {
                fn f(_: []const u8) Error!void {
                    return Error.NotPassConsistencyCheck;
                }
            }.f) catch |err| {
                assert(err == Error.NotPassConsistencyCheck, "expected error NotPassConsistencyCheck", .{});
            };
        }
    }.exec);
}

// Ensure that Tx commit handlers are called after a transaction successfully commits.
test "Tx_OnCommit" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;

    const Context = struct {
        commitCount: usize = 0,
    };
    var ctx = Context{};
    try kvDB.updateWithContext(&ctx, struct {
        fn exec(context: *Context, trx: *tx.TX) Error!void {
            const ctxPtr = @as(*anyopaque, @ptrCast(@alignCast(context)));
            trx.onCommit(ctxPtr, struct {
                fn callback(_ctxPtr: ?*anyopaque, _: *tx.TX) void {
                    const argPtr = @as(*Context, @ptrCast(@alignCast(_ctxPtr)));
                    argPtr.commitCount += 1;
                }
            }.callback);
        }
    }.exec);
    try std.testing.expectEqual(@as(usize, 1), ctx.commitCount);
}

// Ensure that Tx commit handlers are NOT called after a transaction rolls back.
test "Tx_OnCommit_Rollback" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const Context = struct {
        x: usize = 0,
    };
    var ctx = Context{};
    const OnCommit = struct {
        fn callback(_ctxPtr: ?*anyopaque, _: *tx.TX) void {
            std.log.err("callback", .{});
            const argPtr = @as(*Context, @ptrCast(@alignCast(_ctxPtr)));
            argPtr.x += 1;
        }
    };
    kvDB.updateWithContext(&ctx, struct {
        fn exec(context: *Context, trx: *tx.TX) Error!void {
            trx.onCommit(context, OnCommit.callback);
            _ = try trx.createBucket("widgets");
            return Error.NotPassConsistencyCheck;
        }
    }.exec) catch |err| {
        assert(err == Error.NotPassConsistencyCheck, "expected error NotPassConsistencyCheck", .{});
    };
    assert(ctx.x == 0, "expected ctx.x to be 0", .{});
}

// Ensure that the database can be copied to a file path.
test "Tx_CopyFile" {
    std.testing.log_level = .err;
    var tmpDir = tests.createTmpFile();
    defer tmpDir.deinit();

    var originPath: []const u8 = ""; // 明确指定类型为 []const u8
    defer std.testing.allocator.free(originPath);
    {
        var testCtx = try tests.setup(std.testing.allocator);
        const kvDB = testCtx.db;
        const Context = struct {
            fp: std.fs.File,
        };
        var ctx = Context{ .fp = tmpDir.file };
        _ = try kvDB.update(struct {
            fn exec(trx: *tx.TX) Error!void {
                const b = try trx.createBucket("widgets");
                try b.put(KeyPair.init("foo", "bar"));
                try b.put(KeyPair.init("baz", "bat"));
            }
        }.exec);

        _ = try kvDB.viewWithContext(&ctx, struct {
            fn exec(copyCtx: *Context, trx: *tx.TX) Error!void {
                var writer = tx.FileWriter.init(copyCtx.fp);
                _ = try trx.writeToAnyWriter(&writer);
            }
        }.exec);
        originPath = std.testing.allocator.dupe(u8, kvDB.path()) catch unreachable;
        tests.teardownNotDeleteDB(&testCtx);
    }
    {
        var options = defaultOptions;
        options.readOnly = false;
        options.initialMmapSize = 100000 * PageSize;
        const filePath = tmpDir.path(std.testing.allocator);
        defer std.testing.allocator.free(filePath);
        const kvDB = try @import("db.zig").DB.open(std.testing.allocator, filePath, null, options);
        defer kvDB.close() catch unreachable;
        try kvDB.view(struct {
            fn exec(trx: *tx.TX) Error!void {
                const b = trx.getBucket("widgets");
                assert(b != null, "expected bucket 'widgets'", .{});
                const foo = b.?.get("foo");
                assert(std.mem.eql(u8, foo.?, "bar"), "expected 'bar'", .{});
                const baz = b.?.get("baz");
                assert(std.mem.eql(u8, baz.?, "bat"), "expected 'bat'", .{});
            }
        }.exec);
    }
}

// Ensure that Copy handles write errors right.
test "Tx_CopyFile_Error_Meta" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    const Context = struct {
        after: usize = 0,
    };
    var ctx = Context{ .after = 3 * kvDB.pageSize };

    _ = try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            try b.put(consts.KeyPair.init("baz", "bat"));
        }
    }.exec);
    const err = kvDB.viewWithContext(&ctx, struct {
        fn exec(_: *Context, trx: *tx.TX) Error!void {
            var tmpDir = tests.createTmpFile();
            defer tmpDir.deinit();

            const streamWriter = struct {
                after: usize = 0,
                fp: std.fs.File,
                const Self = @This();
                fn init(fp: std.fs.File) Self {
                    return Self{ .after = 0, .fp = fp };
                }

                // Write all bytes to the file, and return the number of bytes written.
                pub fn writeAll(self: *Self, bytes: []const u8) Error!usize {
                    self.fp.writeAll(bytes) catch {
                        return Error.FileIOError;
                    };
                    self.after += bytes.len;
                    if (self.after >= 2 * consts.PageSize) {
                        return Error.FileIOError;
                    }
                    return bytes.len;
                }
            };
            var writer = streamWriter.init(tmpDir.file);
            _ = try trx.writeToAnyWriter(&writer);
        }
    }.exec);
    std.debug.assert(err == Error.FileIOError);
}

// Ensure that Copy handles write errors right.
test "Tx_CopyFile_Error_Normal" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(KeyPair.init("foo", "bar"));
            try b.put(KeyPair.init("baz", "bat"));
        }
    }.exec);

    const err = kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            var tmpDir = tests.createTmpFile();
            defer tmpDir.deinit();

            const streamWriter = struct {
                after: usize = 0,
                fp: std.fs.File,
                const Self = @This();
                fn init(fp: std.fs.File) Self {
                    return Self{ .after = 0, .fp = fp };
                }

                // Write all bytes to the file, and return the number of bytes written.
                pub fn writeAll(self: *Self, bytes: []const u8) Error!usize {
                    self.fp.writeAll(bytes) catch {
                        return Error.FileIOError;
                    };
                    self.after += bytes.len;
                    std.log.info("has written {d} bytes, page size: {d}", .{ self.after, consts.PageSize });
                    if (self.after > 2 * consts.PageSize) {
                        return Error.FileIOError;
                    }
                    return bytes.len;
                }
            };
            var writer = streamWriter.init(tmpDir.file);
            _ = try trx.writeToAnyWriter(&writer);
        }
    }.exec);
    std.debug.assert(err == Error.FileIOError);
}

test "ExampleTx_Rollback" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    // Create a bucket.
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.exec);
    // Set a value for a key.
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            const b = trx.getBucket("widgets").?;
            try b.put(KeyPair.init("foo", "bar"));
        }
    }.exec);
    // Update the key but rollback the transaction so it never saves.
    {
        const trx = try kvDB.begin(true);
        const b = trx.getBucket("widgets").?;
        try b.put(KeyPair.init("foo", "baz"));
        try trx.rollbackAndDestroy();
    }
    // Ensure that our original value is still set.
    try kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            const b = trx.getBucket("widgets").?;
            const foo = b.get("foo");
            assert(foo != null, "expected 'foo' to be set", .{});
            assert(std.mem.eql(u8, foo.?, "bar"), "expected 'bar'", .{});
        }
    }.exec);
}

test "ExampleTx_CopyFile" {
    std.testing.log_level = .err;
    var testCtx = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testCtx);
    const kvDB = testCtx.db;
    // Create a bucket and a key.
    try kvDB.update(struct {
        fn exec(trx: *tx.TX) Error!void {
            _ = try trx.createBucket("widgets");
            const b = trx.getBucket("widgets").?;
            try b.put(KeyPair.init("foo", "bar"));
        }
    }.exec);
    // Copy the database to another file.
    var tmpDir = tests.createTmpFile();
    try kvDB.updateWithContext(tmpDir.file, struct {
        fn exec(ctx: std.fs.File, trx: *tx.TX) Error!void {
            var writer = tx.FileWriter.init(ctx);
            _ = try trx.writeToAnyWriter(&writer);
        }
    }.exec);
    const filePath = tmpDir.path(testCtx.allocator);
    defer tmpDir.deinit();
    defer testCtx.allocator.free(filePath);
    const db = try @import("db.zig").DB.open(testCtx.allocator, filePath, null, defaultOptions);
    defer db.close() catch unreachable;
    // Ensure that the key is still set.
    try kvDB.view(struct {
        fn exec(trx: *tx.TX) Error!void {
            const b = trx.getBucket("widgets").?;
            const foo = b.get("foo");
            assert(foo != null, "expected 'foo' to be set", .{});
            assert(std.mem.eql(u8, foo.?, "bar"), "expected 'bar'", .{});
        }
    }.exec);
}
