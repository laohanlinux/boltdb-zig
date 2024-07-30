const std = @import("std");
const db = @import("./db.zig");
const bucket = @import("./bucket.zig");
const page = @import("./page.zig");
const Cursor = @import("./cursor.zig").Cursor;
const consts = @import("./consts.zig");
const util = @import("./util.zig");
const TxId = consts.TxId;
const Error = @import("./error.zig").Error;
const DB = db.DB;
const Meta = db.Meta;
const Page = page.Page;
const Bucket = bucket.Bucket;
const onCommitFn = util.closure(usize);

// Represent a read-only or read/write transaction on the database.
// Read-only transactions can be used for retriving values for keys and creating cursors.
// Read/Write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with
// them. Pages can not be reclaimed by the writer until no more transactions
// are using them. A long running read transaction can cause the database to
// quickly grow.
pub const TX = struct {
    writable: bool,
    managed: bool,
    db: ?*DB,
    meta: *Meta,
    root: *Bucket,
    pages: std.AutoHashMap(page.PgidType, *Page),
    stats: TxStats,

    _commitHandlers: std.ArrayList(onCommitFn),
    // WriteFlag specifies the flag for write-related methods like WriteTo().
    // Tx opens the database file with the specified flag to copy the data.
    //
    // By default, the flag is unset, which works well for mostly in-memory
    // workloads. For databases that are much larger than available RAM,
    // set the flag to syscall.O_DIRECT to avoid trashing the page cache.
    writeFlag: isize,

    const Self = @This();

    /// Initializes the transaction.
    pub fn init(_db: *DB) *Self {
        const self = _db.allocator.create(Self) catch unreachable;
        self.db = _db;
        self.pages = std.AutoHashMap(page.PgidType, *Page).init(_db.allocator);
        self.stats = TxStats{};
        std.debug.print("the meta copy is \n", .{});

        // Copy the meta page since it can be changed by the writer.
        self.meta = _db.allocator.create(Meta) catch unreachable;
        _db.getMeta().copy(self.meta);
        // Copy over the root bucket.
        self.root = bucket.Bucket.init(self);
        self.root._b.? = self.meta.root;
        self.root.rootNode = null;

        // Increment the transaction id and add a page cache for writable transactions.
        if (self.writable) {
            self.pages = std.AutoHashMap(page.PgidType, *page.Page).init(self.db.?.allocator);
            self.meta.txid += 1;
        }
        self._commitHandlers = std.ArrayList(onCommitFn).init(_db.allocator);
        std.debug.print("tx's root bucket {any}\n", .{self.root._b.?});
        std.debug.print("onCommit init: {}\n", .{self._commitHandlers.capacity});
        return self;
    }

    pub fn destroy(self: *Self) void {
        if (self.db) |_db| {
            _db.allocator.destroy(self);
        }
    }

    /// Returns the current database size in bytes as seen by this transaction.
    pub fn size(self: *const Self) u64 {
        return self.meta.pgid * @as(u64, self.db.?.pageSize);
    }

    /// Creates a cursor assosicated with the root bucket.
    /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(self: *Self) *Cursor {
        return self.root.cursor();
    }

    // Retrives a copy of the current transaction statistics.
    pub fn getStats(self: *const Self) TxStats {
        return self.stats;
    }

    /// Retrieves a bucket any name.
    /// Returns null if the bucekt does not exist.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn getBucket(self: *Self, name: []const u8) ?*bucket.Bucket {
        return self.root.getBucket(name);
    }

    /// Creates a new bucket.
    /// Returns an error if the bucket already exists, if th bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucket(self: *Self, name: []const u8) Error!*bucket.Bucket {
        return self.root.createBucket(name);
    }

    /// Creates a new bucket if the bucket if it doesn't already exist.
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucketIfNotExists(self: *Self, name: []const u8) Error!*bucket.Bucket {
        return self.root.createBucketIfNotExists(name);
    }

    /// Deletes a bucket.
    /// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
    pub fn deleteBucket(self: *Self, name: []const u8) Error!void {
        return self.root.deleteBucket(name);
    }

    /// Executes a function for each bucket in the root.
    /// If the provided function returns an error then the iteration is stopped and
    /// the error is returned to the caller.
    pub fn forEach(self: *Self, f: fn (*bucket.Bucket, *const consts.KeyPair) Error!void) Error!void {
        return self.root.forEach(f);
    }

    /// Adds a handler function to be executed after the transaction successfully commits.
    pub fn onCommit(self: *Self, func: onCommitFn) void {
        self._commitHandlers.append(func) catch unreachable;
    }

    /// Writes all changes to disk and updates the meta page.
    /// Returns an error if a disk write error occurs, or if commit is
    /// called on a ready-only transaction.
    pub fn commit(self: *Self) Error!void {
        defer std.debug.print("finish commit.\n", .{});
        const assert = @import("./assert.zig").assert;
        assert(!self.managed, "mananged tx commit not allowed", .{});
        if (self.db == null) {
            return Error.TxClosed;
        } else if (!self.writable) {
            return Error.TxNotWriteable;
        }

        // TODO(benbjohnson): Use vectorized I/O to write out dirty pages.
        // Rebalance nodes which have had deletions.
        var startTime = std.time.Timer.start() catch unreachable;
        self.root.rebalance();
        if (self.stats.rebalance > 0) {
            self.stats.spill_time += startTime.lap();
        }

        // spill data onto dirty pages.
        startTime = std.time.Timer.start() catch unreachable;
        // 在分裂（比如合并两个节点90%，以及30%，合并后就变成120%，所以先合并，再分裂没问题）
        self.root.spill() catch |err| {
            self._rollback();
            return err;
        };
        self.stats.spill_time += startTime.lap();

        // Free the old root bucket.
        self.meta.root.root = self.root._b.?.root;
        const opgid = self.meta.pgid;

        // Free the freelist and allocate new pages for it. This will overestimate
        // the size of the freelist but not underestimate the size (wich would be bad).
        self.db.?.freelist.free(self.meta.txid, self.getPage(self.meta.freelist)) catch unreachable;
        const p = self.allocate((self.db.?.freelist.size() / self.db.?.pageSize) + 1) catch |err| {
            std.log.err("failed to allocate memory: {}", .{err});
            self._rollback();
            return Error.OutOfMemory;
        };
        self.db.?.freelist.write(p) catch |err| {
            self._rollback();
            return err;
        };
        self.meta.freelist = p.id;

        // If the high water mark has moved up then attempt to grow the database.
        if (self.meta.pgid > opgid) {
            const growSize = @as(usize, (self.meta.pgid + 1)) * self.db.?.pageSize;
            self.db.?.grow(growSize) catch |err| {
                self._rollback();
                return err;
            };
        }

        // Write dirty pages to disk.
        startTime = std.time.Timer.start() catch unreachable;
        self.write() catch |err| {
            self._rollback();
            return err;
        };

        // // If strict mode is enabled then perform a consistency check.
        // // Only the first consistency error is reported in the panic.
        // if (self.db.?.strict_mode) {
        //     // TODO
        // }
        //
        // Write meta to disk.
        self.writeMeta() catch |err| {
            self._rollback();
            return err;
        };
        self.stats.writeTime += startTime.lap();
        std.debug.print("write cost time: {}ms\n", .{self.stats.writeTime / std.time.ns_per_ms});


        // Finalize the transaction.
        self.close();


        // ok
    }

    /// Closes the transaction and ignores all previous updates. Read-only
    /// transactions must be rolled back and not committed.
    pub fn rollback(self: *Self) Error!void {
        const assert = @import("./assert.zig").assert;
        assert(!self.managed, "managed tx rollback not allowed", .{});
        if (self.db == null) {
            return Error.TxClosed;
        }
        self._rollback();
    }

    pub fn write(_: *Self) Error!void {}

    // Writes the meta to the disk.
    fn writeMeta(self: *Self) Error!void {
        // Create a tempory buffer for the meta page.
        var buf = std.ArrayList(u8).initCapacity(self.getDB().allocator, self.getDB().pageSize) catch unreachable;
        buf.appendNTimes(0, self.getDB().pageSize) catch unreachable;
        const p = self.getDB().pageInBuffer(buf.items, 0);
        self.meta.write(p);
        // Write the meta page to file.
        // TODO
    }

    // Internal rollback function.
    pub fn _rollback(self: *Self) void {
        //std.debug.print("transaction executes rollback!\n", .{});
        if (self.db == null) {
            return;
        }
        if (self.writable) {
            self.db.?.freelist.rollback(self.meta.txid);
            self.db.?.freelist.reload(self.db.?.pageById(self.db.?.getMeta().freelist));
        }

        self.close();
    }

    fn close(self: *Self) void {
        if (self.db == null) {
            return;
        }
        if (self.writable) {
            // Grab freelist stats.
            const freelistFreeN = self.getDB().freelist.freeCount();
            const freelistPendingN = self.getDB().freelist.pendingCount();
            const freelistAlloc = self.getDB().freelist.size();

            // Remove transaction ref & writer lock.
            self.getDB().rwtx = null;
            self.getDB().rwlock.unlock();

            // Merge statistics.
            self.getDB().statlock.lock();
            self.getDB().stats.free_page_n = freelistFreeN;
            self.getDB().stats.pending_page_n = freelistPendingN;
            self.getDB().stats.free_alloc = (freelistFreeN + freelistPendingN) + self.getDB().pageSize;
            self.getDB().stats.free_list_inuse = freelistAlloc;
            self.getDB().stats.tx_stats.add(&self.stats);
            self.getDB().statlock.unlock();
        } else {
            self.db.?.removeTx(self);
            std.debug.print("remove tx({}) from db\n", .{self.meta.txid});
        }

        // clear all reference.
        const allocator = self.db.?.allocator;
        self.db.?.allocator.destroy(self.meta);
        self.db = null;
        self.pages.deinit();
        self.root.deinit();
        // Execute commit handlers now that the locks have been removed.
        std.debug.print("execute commit handlers {}\n", .{self._commitHandlers.capacity});
        for (self._commitHandlers.items) |func| {
            func.execute();
        }
        allocator.destroy(self);
    }

    /// Iterates over every page within a given page and executes a function.
    pub fn forEachPage(self: *Self, pgid: page.PgidType, depth: usize, comptime CTX: type, c: CTX, travel: fn (ctx: CTX, p: *const page.Page, depth: usize) void) void {
        const p = self.getPage(pgid);

        // Execute function.
        travel(c, p, depth);

        // Recursively loop over children.
        if (p.flags & consts.intFromFlags(consts.PageFlag.branch) != 0) {
            for (p.branchPageElements().?) |elem| {
                self.forEachPage(elem.pgid, depth + 1, CTX, c, travel);
            }
        }
    }

    /// Returns a reference to the page with a given id.
    /// If page has been written to then a temporary buffered page is returned.
    pub fn getPage(self: *Self, id: page.PgidType) *page.Page {
        // Check the dirty pages first.
        if (self.pages.get(id)) |p| {
            return p;
        }
        // Otherwise return directly form the mmap.
        return self.db.?.pageById(id);
    }

    pub fn getID(self: *const Self) u64 {
        return self.meta.txid;
    }

    pub fn getDB(self: *Self) *db.DB {
        return self.db.?;
    }

    /// Returns current database size in bytes as seen by this transaction.
    pub fn getSize(self: *const Self) usize {
        self.db.pageSize * @as(usize, self.meta.txid);
    }

    pub fn writable(self: *const Self) bool {
        return self.writable;
    }

    pub fn allocate(self: *Self, count: usize) !*page.Page {
        const p = self.db.?.allocatePage(count);
        return p;
    }
};

// Represents statusctics about the actiosn performed by the transaction
pub const TxStats = packed struct {
    // Page statistics.
    page_count: usize = 0, // number of page allocations
    page_alloc: usize = 0, // total bytes allocated

    // Cursor statistics
    cursor_count: usize = 0, // number of cursors created

    // Node statistics.
    nodeCount: usize = 0, // number of node allocations
    nodeDeref: usize = 0, // number of node dereferences

    // Rebalance statstics
    rebalance: usize = 0, // number of node rebalances
    rebalance_time: u64 = 0,

    // Split/Spill statistics
    split: usize = 0, // number of nodes split
    spill: usize = 0, // number of nodes spilled
    spill_time: u64 = 0, // total time spent spilling

    // Write statistics
    write: usize = 0, // number of writes performed
    writeTime: u64 = 0, // total time spent writing to disk

    const Self = @This();
    pub fn add(self: *Self, other: *TxStats) void {
        self.page_count += other.page_count;
        self.page_alloc += other.page_alloc;
        self.cursor_count += other.cursor_count;
        self.nodeCount += other.nodeCount;
        self.nodeDeref += other.nodeDeref;
        self.rebalance += other.rebalance;
        self.rebalance_time += other.rebalance_time;
        self.split += other.split;
        self.spill += other.spill;
        self.spill_time += other.spill_time;
        self.write += other.write;
        self.writeTime += other.writeTime;
    }

    // Calculates and returns the difference between two sets of transaction stats.
    // This is useful when obtaning stats at two different points and time and
    // you need the performance counters that occurred within that time span.
    pub fn sub(self: *Self, other: *TxStats) Self {
        return TxStats{
            .page_count = self.page_count - other.page_count,
            .page_alloc = self.page_alloc - other.page_alloc,
            .cursor_count = self.cursor_count - other.cursor_count,
            .nodeCount = self.nodeCount - other.nodeCount,
            .nodeDeref = self.nodeDeref - other.nodeDeref,
            .rebalance = self.rebalance - other.rebalance,
            .rebalance_time = self.rebalance_time - other.rebalance_time,
            .split = self.split - other.split,
            .spill = self.spill - other.spill,
            .spill_time = self.spill_time - other.spill_time,
            .write = self.write - other.write,
            .writeTime = self.writeTime - other.writeTime,
        };
    }
};

fn tEach(_: void, _: *page.Page, depth: usize) void {
    std.debug.print("Hello Word: {}\n", .{depth});
}

// test "forEach" {
//     const tx = std.testing.allocator.create(TX) catch unreachable;
//     defer std.testing.allocator.destroy(tx);
//     tx.forEach(void, {}, 1, 100, tEach);
// }
