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
    std.testing.log_level = .debug;
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
test "DB-Update_ManagedCommit" {
    // don't need to test this, since the database is not open
}
