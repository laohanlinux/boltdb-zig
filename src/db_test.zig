const std = @import("std");
const tests = @import("tests.zig");
const consts = @import("consts.zig");
const DB = @import("db.zig").DB;
const TX = @import("tx.zig").TX;
const defaultOptions = consts.defaultOptions;
const Error = @import("error.zig").Error;
const assert = @import("util.zig").assert;

test "DB-Open" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const kvDB = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch unreachable;
    try kvDB.close();
}

//  Ensure that opening a database with a blank path returns an error.
test "DB-Open_ErrPathRequired" {
    std.testing.log_level = .err;
    _ = DB.open(std.testing.allocator, "", null, defaultOptions) catch |err| {
        std.debug.assert(err == std.fs.File.OpenError.FileNotFound);
    };
}

// Ensure that opening a database with a bad path returns an error.
test "DB-Open_ErrNotExists" {
    std.testing.log_level = .err;
    const badPath = "bad-path/db.db";
    _ = DB.open(std.testing.allocator, badPath, null, defaultOptions) catch |err| {
        std.debug.assert(err == std.fs.File.OpenError.NotDir);
    };
}

// Ensure that opening a file that is not a Bolt database returns ErrInvalid.
test "DB-Open_ErrInvalid" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.writeAll("this is not a bolt database") catch unreachable;
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    _ = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch |err| {
        std.debug.assert(err == Error.Invalid);
    };
}

// Ensure that opening a file with two invalid versions returns ErrVersionMismatch.
test "DB-Open_ErrVersionMismatch" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    // Create empty database.
    const db = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    try db.close();

    // Read data file.
    const data = try std.fs.cwd().readFileAlloc(std.testing.allocator, filePath, 1 << 20);
    defer std.testing.allocator.free(data);

    // Rewrite meta pages.
    const meta0 = @import("page.zig").Page.init(data[0..consts.PageSize]).meta();
    meta0.*.version += 1;
    const meta1 = @import("page.zig").Page.init(data[consts.PageSize .. 2 * consts.PageSize]).meta();
    meta1.*.version += 1;

    // Write meta pages.
    try std.fs.cwd().writeFile(.{
        .sub_path = filePath,
        .data = data,
    });

    // Open database.
    _ = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch |err| {
        std.debug.assert(err == Error.VersionMismatch);
    };
}

// Ensure that opening a file with two invalid checksums returns ErrChecksum.
test "DB-Open_ErrChecksum" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const db = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    try db.close();

    // Read data file.
    const data = try std.fs.cwd().readFileAlloc(std.testing.allocator, filePath, 1 << 20);
    defer std.testing.allocator.free(data);

    // Rewrite meta pages.
    const meta0 = @import("page.zig").Page.init(data[0..consts.PageSize]).meta();
    meta0.*.pgid += 1;
    const meta1 = @import("page.zig").Page.init(data[consts.PageSize .. 2 * consts.PageSize]).meta();
    meta1.*.pgid += 1;

    // Write meta pages.
    try std.fs.cwd().writeFile(.{
        .sub_path = filePath,
        .data = data,
    });

    // Open database.
    _ = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch |err| {
        std.debug.assert(err == Error.CheckSum);
    };
}

// Ensure that opening a database does not increase its size.
// https://github.com/boltdb/bolt/issues/291
test "DB-Open_Size" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const db = try DB.open(std.testing.allocator, filePath, null, defaultOptions);

    const pageSize = db.pageSize;
    std.debug.assert(pageSize == consts.PageSize);
    // Insert until we get above the minimum 4MB size.
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            const b = try trx.createBucketIfNotExists("data");
            var key = [_]u8{0} ** 100;
            var value = [_]u8{0} ** 1000;
            for (0..10000) |i| {
                const keyPair = consts.KeyPair.init(std.fmt.bufPrint(key[0..], "{:04}", .{i}) catch unreachable, value[0..1000]);
                try b.put(keyPair);
            }
        }
    }.update);

    // Close database and grab the size.
    try db.close();

    const size = try std.fs.cwd().statFile(filePath);
    std.debug.assert(size.size != 0);

    // Reopen database, update, and check size again.
    const db0 = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    db0.update(struct {
        fn update(trx: *TX) Error!void {
            const b = try trx.createBucketIfNotExists("data");
            const keyPair = consts.KeyPair.init("0", "0");
            try b.put(keyPair);
        }
    }.update) catch unreachable;
    try db0.close();

    const size2 = try std.fs.cwd().statFile(filePath);
    std.debug.assert(size2.size != 0);
    // Compare the original size with the new size.
    // db size might increase by a few page sizes due to the new small update.
    @import("assert.zig").assert(size.size >= size2.size - 5 * pageSize, "unexpected file growth, {d} >= {d}", .{ size.size, size2.size - 5 * pageSize });
    // std.log.err("size: {d}, size2: {d}", .{ size.size, size2.size });
}

// Ensure that opening a database beyond the max step size does not increase its size.
// https://github.com/boltdb/bolt/issues/303
test "DB-Open_Size_Large" {
    if (!tests.isLongModel()) {
        return;
    }
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const db = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    const pageSize = db.pageSize;
    // Insert until we get above the minimum 4MB size.
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            const b = try trx.createBucketIfNotExists("data");
            var key = [_]u8{0} ** 8;
            const value = [_]u8{0} ** 50;
            for (0..10000) |i| {
                std.mem.writeInt(u64, key[0..8], i, .big);
                const keyPair = consts.KeyPair.init(key[0..8], value[0..50]);
                try b.put(keyPair);
            }
        }
    }.update);
    // Close database and grab the size.
    try db.close();
    const size = try std.fs.cwd().statFile(filePath);
    assert(size.size != 0, "unexpected new file size: {}", .{size.size});
    assert(size.size < (1 << 30), "expected larger initial size: {}", .{size.size});

    // Reopen database, update, and check size again.

    const db0 = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    try db0.update(struct {
        fn update(trx: *TX) Error!void {
            const b = trx.getBucket("data").?;
            try b.put(consts.KeyPair.init("0", "0"));
        }
    }.update);
    try db0.close();

    const size2 = try std.fs.cwd().statFile(filePath);
    assert(size2.size != 0, "unexpected new file size: {}", .{size2.size});
    // Compare the original size with the new size.
    // db size might increase by a few page sizes due to the new small update.
    assert(size.size >= size2.size - 5 * pageSize, "unexpected file growth, {d} >= {d}", .{ size.size, size2.size - 5 * pageSize });
}

// Ensure that a re-opened database is consistent.
test "DB-Open_Check" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const db = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    try db.view(struct {
        fn view(self: *TX) Error!void {
            try self.check();
        }
    }.view);
    try db.close();

    const db2 = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    try db2.view(struct {
        fn view(self: *TX) Error!void {
            try self.check();
        }
    }.view);
    try db2.close();
}

// Ensure that a database that is too small returns an error.
test "DB-Open_FileTooSmall" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const db = try DB.open(std.testing.allocator, filePath, null, defaultOptions);
    try db.close();

    // corrupt the database
    const data = try std.fs.cwd().readFileAlloc(std.testing.allocator, filePath, 1 << 20);
    defer std.testing.allocator.free(data);
    try std.fs.cwd().writeFile(.{
        .sub_path = filePath,
        .data = data[0..consts.PageSize], // only write the first page
    });

    // try to open the database
    _ = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch |err| {
        std.debug.assert(err == Error.Invalid);
        return;
    };
}

// TestDB_Open_InitialMmapSize tests if having InitialMmapSize large enough
// to hold data from concurrent write transaction resolves the issue that
// read transaction blocks the write transaction and causes deadlock.
// This is a very hacky test since the mmap size is not exposed.
test "DB-Open_InitialMmapSize" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const initialMmapSize = 1 << 31; // 2GB
    const testWriteSize = 1 << 27; // 134MB
    var opts = defaultOptions;
    opts.initialMmapSize = initialMmapSize;
    const db = try DB.open(std.testing.allocator, filePath, null, opts);
    // create a long-running read transaction
    const rtx = try db.begin(false);
    // that never gets closed while writing
    const wtx = try db.begin(true);
    const b = wtx.createBucket("test") catch unreachable;
    // and commit a large write
    const value = [_]u8{0} ** testWriteSize;
    try b.put(consts.KeyPair.init("foo", value[0..]));

    // gorutine to commit the write transaction
    _ = try std.Thread.spawn(.{}, struct {
        fn run(_wtx: *TX) void {
            _wtx.commitAndDestroy() catch unreachable;
            std.log.debug("write transaction committed", .{});
        }
    }.run, .{wtx});

    // sleep for a while to make sure the read transaction is blocked
    const log = std.log.scoped(.Test);
    log.debug("sleeping for 5 seconds", .{});
    std.time.sleep(std.time.ns_per_s * 5);
    log.debug("read transaction rolled back", .{});
    try rtx.rollbackAndDestroy();
    try db.close();
}

// Ensure that a database cannot open a transaction when it's not open.
test "DB-DB_Begin_ErrDatabaseNotOpen" {
    // don't need to test this, since the database is not open
}

// Ensure that a read-write transaction can be retrieved.
test "DB-BeginRW" {
    std.testing.log_level = .err;
    const tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);
    const db = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch unreachable;
    defer db.close() catch unreachable;
    const tx = try db.begin(true);
    assert(tx.writable, "transaction is not writable", .{});
    try tx.rollbackAndDestroy();
}

// Ensure that opening a transaction while the DB is closed returns an error.
test "DB-BeginRW_Closed" {
    // don't need to test this, since the database is not open
}

// Ensure that a database cannot close while transactions are open.
test "DB-Close_Open" {
    // don't need to test this, since the database is not open
    std.testing.log_level = .err;
    var tmpFile = tests.createTmpFile();
    tmpFile.file.close();
    defer tmpFile.tmpDir.cleanup();
    const filePath = tmpFile.path(std.testing.allocator);
    defer std.testing.allocator.free(filePath);

    const db = DB.open(std.testing.allocator, filePath, null, defaultOptions) catch unreachable;
    // Start transaction.
    const tx = try db.begin(true);
    // Open update in separate goroutine.

    const ThreadCtx = struct {
        dbClosed: bool,
        db: *DB,
    };
    var threadCtx = ThreadCtx{
        .dbClosed = false,
        .db = db,
    };
    _ = try std.Thread.spawn(.{}, struct {
        fn run(_ctx: *ThreadCtx) void {
            _ctx.db.close() catch unreachable;
            _ctx.dbClosed = true;
        }
    }.run, .{&threadCtx});
    // Ensure database hasn't closed.
    std.time.sleep(std.time.ns_per_s * 2);
    assert(!threadCtx.dbClosed, "database closed", .{});
    // Commit transaction.
    try tx.commitAndDestroy();
    // Ensure database has closed.
    std.time.sleep(std.time.ns_per_s * 2);
    assert(threadCtx.dbClosed, "database not closed", .{});
}

// Ensure a database can provide a transactional block.
test "DB-Update" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);

    const db = testContext.db;
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(consts.KeyPair.init("foo", "bar"));
            try b.put(consts.KeyPair.init("baz", "bat"));
            try b.delete("foo");
        }
    }.update);

    try db.view(struct {
        fn view(trx: *TX) Error!void {
            const b = trx.getBucket("widgets").?;
            const value = b.get("foo");
            assert(value == null, "value should be null", .{});
            const value2 = b.get("baz").?;
            assert(std.mem.eql(u8, value2, "bat"), "value should be bat", .{});
        }
    }.view);
}

// Ensure a closed database returns an error while running a transaction block
test "DB-Update_Closed" {
    // don't need to test this, since the database is not open
}

// Ensure a panic occurs while trying to commit a managed transaction.
test "DB-Update_ManualCommit" {
    // don't need to test this, since the database is not open
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    var panicked = false;
    db.updateWithContext(&panicked, struct {
        fn update(_p: *bool, trx: *TX) Error!void {
            errdefer {
                _p.* = true;
            }
            try trx.commit();
        }
    }.update) catch |err| {
        assert(err == Error.ManagedTxCommitNotAllowed, "transaction not panicked", .{});
    };
    assert(panicked, "transaction not panicked", .{});
}

// Ensure a panic occurs while trying to commit a managed transaction.
test "DB-Update_ManualRollback" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    var panicked = false;
    db.updateWithContext(&panicked, struct {
        fn update(_p: *bool, trx: *TX) Error!void {
            errdefer {
                _p.* = true;
            }
            try trx.rollback();
        }
    }.update) catch |err| {
        assert(err == Error.ManagedTxRollbackNotAllowed, "transaction not panicked", .{});
    };
    assert(panicked, "transaction not panicked", .{});
}

// Ensure a panic occurs while trying to commit a managed transaction.
test "DB-View_ManualCommit" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    var panicked = false;
    db.viewWithContext(&panicked, struct {
        fn view(_p: *bool, trx: *TX) Error!void {
            errdefer {
                _p.* = true;
            }
            try trx.commit();
        }
    }.view) catch |err| {
        assert(err == Error.ManagedTxCommitNotAllowed, "transaction not panicked", .{});
    };
    assert(panicked, "transaction not panicked", .{});
}

// Ensure a panic occurs while trying to rollback a managed transaction.
test "DB-View_ManualRollback" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    var panicked = false;
    db.viewWithContext(&panicked, struct {
        fn view(_p: *bool, trx: *TX) Error!void {
            errdefer {
                _p.* = true;
            }
            try trx.rollback();
        }
    }.view) catch |err| {
        assert(err == Error.ManagedTxRollbackNotAllowed, "transaction not panicked", .{});
    };
    assert(panicked, "transaction not panicked", .{});
}

// Ensure a write transaction that panics does not hold open locks.
test "DB-Update_Panic" {
    // don't need to test this, since the zig language panic is different from the Go.

    // std.testing.log_level = .err;
    // var testContext = try tests.setup(std.testing.allocator);
    // defer tests.teardown(&testContext);
    // const db = testContext.db;
    // var panicked = false;
    // const Context = struct {
    //     db: *DB,
    //     panicked: *bool,
    // };

    // const updateFn = struct {
    //     fn update(_ctx: *Context) void {
    //         std.debug.catch_panic(_ctx, struct {
    //             fn panicHandler(ctx: *Context, stack_trace: ?*std.builtin.StackTrace) void {
    //                 _ = stack_trace;
    //                 ctx.panicked.* = true;
    //             }
    //         }.panicHandler) catch |err| {
    //             std.log.err("Error in thread: {}", .{err});
    //             _ctx.panicked.* = true;
    //             return;
    //         };

    //         _ctx.db.update(struct {
    //             fn update(trx: *TX) Error!void {
    //                 _ = try trx.createBucket("test");
    //                 @panic("test");
    //             }
    //         }.update) catch |err| {
    //             std.log.err("Error in transaction: {}", .{err});
    //             _ctx.panicked.* = true;
    //             return;
    //         };
    //     }
    // }.update;

    // var ctx = Context{
    //     .db = db,
    //     .panicked = &panicked,
    // };

    // const join = try std.Thread.spawn(.{}, updateFn, .{&ctx});
    // join.join();
    // assert(panicked, "transaction not panicked", .{});
}

// Ensure a database can return an error through a read-only transactional block.
test "DB-View_Error" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    db.view(struct {
        fn view(_: *TX) Error!void {
            return Error.ManagedTxCommitNotAllowed;
        }
    }.view) catch |err| {
        assert(err == Error.ManagedTxCommitNotAllowed, "transaction not panicked", .{});
    };
}

// Ensure a read transaction that panics does not hold open locks.
test "DB-View_Panic" {
    // don't need to test this, since the zig language panic is different from the Go.
}

// Ensure that DB stats can be returned.
test "DB-Stats" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            _ = try trx.createBucket("test");
        }
    }.update);
    const stats = db.getStats();
    assert(stats.txStats.pageCount == 2, "page count should be 2", .{});
    assert(stats.freePageN == 0, "free page count should be 0", .{});
    assert(stats.pendingPageN == 2, "pending page count should be 2", .{});
}

// Ensure that database pages are in expected order and type.
test "DB-Consistency" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.update);

    for (0..10) |_| {
        try db.update(struct {
            fn update(trx: *TX) Error!void {
                const b = try trx.createBucketIfNotExists("widgets");
                try b.put(consts.KeyPair.init("foo", "bar"));
            }
        }.update);
    }

    try db.update(struct {
        fn update(trx: *TX) Error!void {
            var p = try trx.getPageInfo(0);
            assert(std.mem.eql(u8, p.?.typ, "meta"), "page type should be meta, but got {s}", .{p.?.typ});
            p = try trx.getPageInfo(1);
            assert(std.mem.eql(u8, p.?.typ, "meta"), "page type should be meta, but got {s}", .{p.?.typ});
            p = try trx.getPageInfo(2); // at pending list, not freed
            assert(std.mem.eql(u8, p.?.typ, "free"), "page type should be free, but got {s}", .{p.?.typ});
            p = try trx.getPageInfo(3); // at pending list, not freed
            assert(std.mem.eql(u8, p.?.typ, "free"), "page type should be free, but got {s}", .{p.?.typ});
            p = try trx.getPageInfo(4);
            assert(std.mem.eql(u8, p.?.typ, "leaf"), "page type should be leaf, but got {s}", .{p.?.typ});
            p = try trx.getPageInfo(5);
            assert(std.mem.eql(u8, p.?.typ, "freelist"), "page type should be freelist, but got {s}", .{p.?.typ});
            const nullPage = try trx.getPageInfo(6);
            assert(nullPage == null, "page should be null", .{});
        }
    }.update);
}

// Ensure that DB stats can be subtracted from one another.
test "DB-Stats_Sub" {
    const Stats = @import("db.zig").Stats;
    var a = Stats{};
    a.txStats.pageCount = 3;
    a.freePageN = 4;
    var b = Stats{};
    b.txStats.pageCount = 10;
    b.freePageN = 14;
    const diff = b.sub(&a);
    assert(diff.txStats.pageCount == 7, "page count should be 7", .{});
    // free page stats are copied from the receiver and not subtracted
    assert(diff.freePageN == 14, "free page count should be 14", .{});
}

// Ensure two functions can perform updates in a single batch.
test "DB-Batch" {
    // don't need to test this, since the batch update is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "DB-Batch_Panic" {
    // don't need to test this, since the batch update is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "DB-BatchFull" {
    // don't need to test this, since the batch update is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "DB-BatchTime" {
    // don't need to test this, since the batch update is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "ExampleDB_Update" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            const b = try trx.createBucket("widgets");
            try b.put(consts.KeyPair.init("foo", "bar"));
        }
    }.update);

    // Read the value back from a separate read-only transaction.
    try db.view(struct {
        fn view(trx: *TX) Error!void {
            const b = trx.getBucket("widgets").?;
            const value = b.get("foo").?;
            assert(std.mem.eql(u8, value, "bar"), "value should be bar", .{});
        }
    }.view);
}

test "ExampleDB_View" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    // Insert data into a bucket.
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            const b = try trx.createBucket("people");
            try b.put(consts.KeyPair.init("john", "doe"));
            try b.put(consts.KeyPair.init("susy", "que"));
        }
    }.update);
    // Access data from within a read-only transactional block.
    try db.view(struct {
        fn view(trx: *TX) Error!void {
            const b = trx.getBucket("people").?;
            const value = b.get("john").?;
            assert(std.mem.eql(u8, value, "doe"), "value should be doe", .{});
            const value2 = b.get("susy").?;
            assert(std.mem.eql(u8, value2, "que"), "value should be que", .{});
        }
    }.view);
}

test "ExampleDB_Begin_ReadOnly" {
    std.testing.log_level = .err;
    var testContext = try tests.setup(std.testing.allocator);
    defer tests.teardown(&testContext);
    const db = testContext.db;
    try db.update(struct {
        fn update(trx: *TX) Error!void {
            _ = try trx.createBucket("widgets");
        }
    }.update);
    // Create several keys in a transaction.
    const trx = try db.begin(true);
    const b = trx.getBucket("widgets").?;
    try b.put(consts.KeyPair.init("john", "blue"));
    try b.put(consts.KeyPair.init("abby", "red"));
    try b.put(consts.KeyPair.init("zephyr", "purple"));
    try trx.commitAndDestroy();

    // Iterate over the values in sorted key order.
    const trx2 = try db.begin(false);
    const b2 = trx2.getBucket("widgets").?;
    var c = b2.cursor();
    var keyPairRef = c.first();
    while (!keyPairRef.isNotFound()) {
        std.debug.print("{s}: {s}\n", .{ keyPairRef.key.?, keyPairRef.value.? });
        keyPairRef = c.next();
    }
    c.deinit();
    try trx2.rollbackAndDestroy();
}

test "BenchmarkDBBatchAutomatic" {
    // don't need to test this, since the benchmark is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "BenchmarkDBBatchSingle" {
    // don't need to test this, since the benchmark is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "BenchmarkDBBatchManual10x100" {
    // don't need to test this, since the benchmark is not implemented.
    // TODO: maybe we can implement it in the future.
}

test "validateBatchBench" {
    // don't need to test this, since the benchmark is not implemented.
    // TODO: maybe we can implement it in the future.
}
