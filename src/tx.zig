const std = @import("std");
const db = @import("db.zig");
const bucket = @import("bucket.zig");
const page = @import("page.zig");
const Cursor = @import("cursor.zig").Cursor;
const consts = @import("consts.zig");
const util = @import("util.zig");
const TxId = consts.TxId;
const Error = @import("error.zig").Error;
const DB = db.DB;
const Meta = db.Meta;
const Page = page.Page;
const Bucket = bucket.Bucket;
const onCommitFn = util.Closure(usize);
const assert = @import("assert.zig").assert;
const Table = @import("pretty_table.zig").Table;
const PgidType = consts.PgidType;
const AutoFreeObject = bucket.AutoFreeObject;

/// Represent a read-only or read/write transaction on the database.
/// Read-only transactions can be used for retriving values for keys and creating cursors.
/// Read/Write transactions can create and remove buckets and create and remove keys.
///
/// IMPORTANT: You must commit or rollback transactions when you are done with
/// them. Pages can not be reclaimed by the writer until no more transactions
/// are using them. A long running read transaction can cause the database to
/// quickly grow.
pub const TX = struct {
    writable: bool,
    managed: bool,
    db: ?*DB,
    meta: *Meta,
    root: *Bucket, // the root bucket of the transaction, the bucket root is reference to the meta.root
    pages: ?*std.AutoHashMap(PgidType, *Page),
    autoFreeNodes: AutoFreeObject,
    stats: TxStats,

    _commitHandlers: std.ArrayList(onCommitFn),
    // WriteFlag specifies the flag for write-related methods like WriteTo().
    // Tx opens the database file with the specified flag to copy the data.
    //
    // By default, the flag is unset, which works well for mostly in-memory
    // workloads. For databases that are much larger than available RAM,
    // set the flag to syscall.O_DIRECT to avoid trashing the page cache.
    writeFlag: isize = 0,

    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initializes the transaction.
    pub fn init(_db: *DB, writable: bool) *Self {
        const self = _db.allocator.create(Self) catch unreachable;
        self.db = _db;
        self.writable = writable;
        self.pages = _db.allocator.create(std.AutoHashMap(PgidType, *Page)) catch unreachable;
        self.pages.?.* = std.AutoHashMap(PgidType, *Page).init(_db.allocator);
        self.stats = TxStats{};
        // Copy the meta page since it can be changed by the writer.
        self.meta = _db.allocator.create(Meta) catch unreachable;
        _db.getMeta().copy(self.meta);
        // Copy over the root bucket.
        self.root = bucket.Bucket.init(self);
        self.root._b.? = self.meta.root;
        assert(self.root._b.?.root == _db.getMeta().root.root, "root is invalid", .{});
        assert(self.root._b.?.sequence == _db.getMeta().root.sequence, "sequence is invalid", .{});
        // Note: here the root node is not set
        self.root.rootNode = null;
        self.autoFreeNodes = AutoFreeObject.init(_db.allocator);
        self.allocator = _db.allocator;
        // Increment the transaction id and add a page cache for writable transactions.
        if (self.writable) {
            self.meta.txid += 1;
        }
        self._commitHandlers = std.ArrayList(onCommitFn).init(self.allocator);
        return self;
    }

    /// Destroys the transaction and releases all associated resources.
    pub fn destroy(self: *Self) void {
        assert(self.db == null, "db should be null before destroy", .{});
        self.allocator.destroy(self);
    }

    /// Print the transaction information.
    pub fn print(self: *const Self) !void {
        var table = Table.init(self.allocator, 24, .Cyan, "Transaction Information");
        defer table.deinit();

        try table.addHeader(.{ "Field", "Value" });

        var buf: [1024]u8 = undefined;

        var nodesKeys = std.ArrayList(u8).init(self.allocator);
        defer nodesKeys.deinit();
        try nodesKeys.appendSlice(try std.fmt.bufPrint(&buf, "count:{}\t", .{self.root.nodes.count()}));
        var nodesItr = self.root.nodes.iterator();
        while (nodesItr.next()) |kv| {
            try nodesKeys.appendSlice(try std.fmt.bufPrint(&buf, "pid:{d}\t", .{kv.key_ptr.*}));
            if (kv.value_ptr.*.key) |key| {
                try nodesKeys.appendSlice(try std.fmt.bufPrint(&buf, "key:{s}\t", .{key}));
            }
        }

        const fields = .{
            .{ "Root.Bucket.root", self.root._b.?.root },
            .{ "Root.Bucket.sequence", self.root._b.?.sequence },
            .{ "Root.Bucket.nodes", nodesKeys.items },
            .{ "Tx.Page is null", self.root.page == null },
            .{ "meta.Root.Root", self.meta.root.root },
            .{ "meta.Root.Sequence", self.meta.root.sequence },
            .{ "meta.Version", self.meta.version },
            .{ "meta.Page Size", self.meta.pageSize },
            .{ "meta.Flags", self.meta.flags },
            .{ "meta.CheckSum", self.meta.check_sum },
            .{ "meta.Magic", self.meta.magic },
            .{ "meta.TxId", self.meta.txid },
            .{ "meta.Freelist.PgId", self.meta.freelist },
            .{ "meta.Freelist.Count", self.db.?.freelist.count() },
            .{ "meta.Freelist.Ids", self.db.?.freelist.ids.items },
            .{ "meta.Pgid", self.meta.pgid },
        };

        inline for (fields) |field| {
            const value = switch (@TypeOf(field[1])) {
                u64, usize, u8, u16, u32, u128, i64, isize, i8, i16, i32, i128 => try std.fmt.bufPrint(&buf, "{d}", .{field[1]}),
                bool => try std.fmt.bufPrint(&buf, "{}", .{field[1]}),
                []u8 => try std.fmt.bufPrint(&buf, "{s}", .{field[1]}),
                []const u8 => try std.fmt.bufPrint(&buf, "{s}", .{field[1]}),
                else => try std.fmt.bufPrint(&buf, "0x{X:0>8}", .{field[1]}),
            };
            try table.addRow(&.{ field[0], value });
        }

        try table.addRow(&.{ "Writable", if (self.writable) "Yes" else "No" });

        try table.print();
    }

    /// Returns the current database size in bytes as seen by this transaction.
    pub fn size(self: *const Self) u64 {
        return self.meta.pgid * @as(u64, self.getDB().pageSize);
    }

    /// Creates a cursor assosicated with the root bucket.
    /// All items in the cursor will return a nil value because all root bucket keys point to buckets.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(self: *Self) *Cursor {
        return self.root.cursor();
    }

    /// Retrives a copy of the current transaction statistics.
    pub fn getStats(self: *const Self) TxStats {
        return self.stats;
    }

    /// Returns the inner's allocator, it will be use to create KeyPair avoid to `copy` outlive memeory.
    pub fn getAllocator(self: *const Self) std.mem.Allocator {
        return self.allocator;
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
        defer assert(self.root._b.?.root == self.meta.root.root, "root is invalid", .{});
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
    pub fn forEach(self: *Self, context: anytype, f: fn (@TypeOf(context), name: []const u8, b: ?*Bucket) Error!void) Error!void {
        const travel = struct {
            fn inner(ctx: @TypeOf(context), key: []const u8, _: ?[]const u8) Error!void {
                const b = ctx._tx.getBucket(key);
                return f(ctx, key, b);
            }
        };
        return self.root.forEachKeyValue(context, travel.inner);
    }

    /// Adds a handler function to be executed after the transaction successfully commits.
    pub fn onCommit(self: *Self, func: onCommitFn) void {
        self._commitHandlers.append(func) catch unreachable;
    }

    /// Writes all changes to disk and updates the meta page.
    /// Returns an error if a disk write error occurs, or if commit is
    /// called on a ready-only transaction.
    pub fn commit(self: *Self) Error!void {
        defer std.log.debug("finish commit", .{});
        assert(!self.managed, "mananged tx commit not allowed", .{});
        if (self.db == null) {
            return Error.TxClosed;
        } else if (!self.writable) {
            return Error.TxNotWriteable;
        }
        const _db = self.getDB();
        // std.log.debug("before commit: {any}", .{self.root._b.?});
        self.print() catch |err| {
            std.log.err("Failed to print transaction info: {any}", .{err});
        };
        // TODO(benbjohnson): Use vectorized I/O to write out dirty pages.
        // Rebalance nodes which have had deletions.
        var startTime = std.time.Timer.start() catch unreachable;
        self.root.rebalance();
        if (self.stats.rebalance > 0) {
            self.stats.spill_time += startTime.lap();
        }

        // spill data onto dirty pages.
        startTime = std.time.Timer.start() catch unreachable;
        // During splitting (for example, merging two nodes at 90% and 30% will become 120% after merging, so merging first and then splitting is no problem)
        if (self.root.rootNode) |rootNode| {
            assert(rootNode.isLeaf, "rootNode should be leaf node", .{});
        }
        // assert(self.root.rootNode != null, "rootNode should be null", .{});
        self.root.spill() catch |err| {
            self._rollback();
            return err;
        };

        // std.log.debug("after commit root spill", .{});
        self.stats.spill_time += startTime.lap();

        // Free the old root bucket.
        self.meta.root.root = self.root._b.?.root;
        const opgid = self.meta.pgid;

        // Free the freelist and allocate new pages for it. This will overestimate
        // the size of the freelist but not underestimate the size (wich would be bad).
        _db.freelist.free(self.meta.txid, self.getPage(self.meta.freelist)) catch unreachable;
        std.log.debug("after freelist free transaction dirty pages", .{});
        const allocatePageCount = _db.freelist.size() / _db.pageSize + 1;
        const p = self.allocate(allocatePageCount) catch |err| {
            std.log.err("failed to allocate memory: {}", .{err});
            self._rollback();
            return Error.OutOfMemory;
        };
        assert(p.id >= 0 and p.flags >= 0, "Santy check!", .{});
        _db.freelist.write(p) catch |err| {
            self._rollback();
            return err;
        };
        self.meta.freelist = p.id;

        // If the high water mark has moved up then attempt to grow the database.
        if (self.meta.pgid > opgid) {
            const growSize = @as(usize, (self.meta.pgid + 1)) * self.db.?.pageSize;
            _db.grow(growSize) catch |err| {
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

        // If strict mode is enabled then perform a consistency check.
        // Only the first consistency error is reported in the panic.
        if (_db.strictMode) {
            self.check() catch |err| util.panicFmt("consistency check failed: {any}", .{err});
        }
        // Write meta to disk.
        self.writeMeta() catch |err| {
            self._rollback();
            return err;
        };
        self.stats.writeTime += startTime.lap();
        std.log.info("write cost time: {}ms", .{self.stats.writeTime / std.time.ns_per_ms});
        // Finalize the transaction.
        self.close();
        std.log.debug("after close transaction.", .{});
    }

    /// Closes the transaction and ignores all previous updates. Read-only
    /// transactions must be rolled back and not committed.
    pub fn rollback(self: *Self) Error!void {
        assert(!self.managed, "managed tx rollback not allowed", .{});
        if (self.db == null) {
            return Error.TxClosed;
        }
        self._rollback();
    }

    /// Writes any dirty pages to disk.
    pub fn write(self: *Self) Error!void {
        // Sort pages by id.
        var pagesSlice = try std.ArrayList(*page.Page).initCapacity(self.allocator, self.pages.?.count());
        defer pagesSlice.deinit();
        var itr = self.pages.?.valueIterator();
        while (itr.next()) |pg| {
            try pagesSlice.append(pg.*);
        }
        const asc = struct {
            fn inner(_: void, a: *Page, b: *Page) bool {
                return a.id > b.id;
            }
        }.inner;
        std.mem.sort(*Page, pagesSlice.items, {}, asc);
        std.log.info("ready to write dirty pages into disk, page count: {}", .{pagesSlice.items.len});
        const _db = self.db.?;
        const opts = _db.opts.?;
        for (pagesSlice.items) |p| {
            // Write out page in 'max allocation' sized chunks.
            const slice = p.asSlice();
            const offset = p.id * @as(u64, _db.pageSize);
            _ = try opts(_db.file, slice, offset);
            // Update statistics
            self.stats.write += 1;
            if (p.id == 117) {
                const k1 = p.branchPageElement(0).?;
                const k2 = p.branchPageElement(1).?;
                std.log.debug("k1: {any}, k2: {any}\n", .{ k1, k2 });
            }
            std.log.debug("write page into disk: pgid: {}, flags: {}, offset: {}, size: {}, any: {any}", .{ p.id, consts.toFlags(p.flags), offset, slice.len, slice });
        }

        // Ignore file sync if flag is set on DB.
        if (!_db.noSync) {
            _db.file.sync() catch unreachable;
        }
    }

    /// Check performs several consistency checks on the database for this transaction.
    /// An error is returned if any inconsistency is found.
    ///
    /// It can be safely run concurrently on a writable transaction. However, this
    /// incurs a hight cost for large databases and databases with a lot of subbuckets.
    /// because of caching. This overhead can be removed if running on a read-only
    /// transaction. however, this is not safe to execute other writer-transactions at
    /// the same time.
    pub fn check(self: *Self) Error!void {
        return self._check();
    }

    fn _check(self: *Self) Error!void {
        // Check if any pages are double freed.
        var reachable = std.AutoHashMap(PgidType, *const page.Page).init(self.allocator);
        defer reachable.deinit();
        var freed = std.AutoHashMap(PgidType, bool).init(self.allocator);
        defer freed.deinit();
        var all = std.ArrayList(PgidType).init(self.allocator);
        defer all.deinit();
        all.appendNTimes(0, self.db.?.freelist.count()) catch unreachable;
        self.db.?.freelist.copyAll(all.items);
        for (all.items) |pgid| {
            if (freed.contains(pgid)) {
                util.panicFmt("page {}: already freed", .{pgid});
            }
            freed.put(pgid, true) catch unreachable;
        }

        // Track every reachable page.
        reachable.put(0, self.getPage(0)) catch unreachable;
        reachable.put(1, self.getPage(1)) catch unreachable;
        // free page
        const freePage = self.getPage(self.meta.freelist);
        for (0..freePage.overflow + 1) |i| {
            const id = self.meta.freelist + i;
            reachable.put(id, self.getPage(self.meta.freelist)) catch unreachable;
        }

        // Recursively check buckets.
        try self.checkBucket(self.root, &reachable, &freed);

        // Ensure all pages below high water mark are either reachable or freed.
        for (0..self.meta.pgid) |i| {
            const isReachable = reachable.contains(i);
            if (!isReachable and !freed.contains(i)) {
                util.panicFmt("page {}: unreachable unfreed", .{i});
            }
        }
    }

    // TODO(benbjohnson): This function is not correct. It should be checking.
    fn checkBucket(self: *Self, b: *bucket.Bucket, reachable: *std.AutoHashMap(PgidType, *const Page), freed: *std.AutoHashMap(consts.PgidType, bool)) Error!void {
        // Ignore inline buckets.
        if (b._b.?.root == 0) {
            return;
        }

        // Check every page used by this bucket.
        const Context = struct {
            reachable: *std.AutoHashMap(PgidType, *const Page),
            freed: *std.AutoHashMap(PgidType, bool),
            _tx: *Self,
        };
        const ctx = Context{ .reachable = reachable, .freed = freed, ._tx = self };
        b.tx.?.forEachPage(
            b._b.?.root,
            0,
            ctx,
            struct {
                fn inner(context: Context, p: *const page.Page, _: usize) void {
                    if (p.id > context._tx.meta.pgid) {
                        util.panicFmt("page id is out of range: {} > {}", .{ p.id, context._tx.meta.pgid });
                    }
                    // Ensure each page is only referenced once.
                    for (0..p.overflow + 1) |i| {
                        const id = p.id + i;
                        if (context.reachable.contains(id)) {
                            util.panicFmt("page {}: multiple references to the same page", .{id});
                        }
                        context.reachable.put(id, p) catch unreachable;
                    }
                    // We should only encounter un-freed leaf and branch pages.
                    if (context.freed.contains(p.id)) {
                        util.panicFmt("page {}: reachable freed", .{p.id});
                    } else if ((consts.intFromFlags(.branch) & p.flags == 0 and consts.intFromFlags(.leaf) & p.flags == 0)) {
                        util.panicFmt("page {}: not a leaf or branch page", .{p.id});
                    }
                }
            }.inner,
        );

        // Check each bucket within this bucket.
        return self.forEach(ctx, struct {
            fn inner(context: Context, name: []const u8, b1: ?*Bucket) Error!void {
                std.log.debug("\t<<<check bucket:[{s}] start>>>", .{name});
                const child = b1.?.getBucket(name);
                if (child) |childBucket| {
                    try childBucket.tx.?.checkBucket(childBucket, context.reachable, context.freed);
                }

                std.log.debug("\t>>>check bucket:[{s}] done<<<", .{name});
                return;
            }
        }.inner);
    }

    // Writes the meta to the disk.
    fn writeMeta(self: *Self) Error!void {
        // Create a tempory buffer for the meta page.
        const _db = self.getDB();
        var buf = std.ArrayList(u8).initCapacity(self.allocator, _db.pageSize) catch unreachable;
        defer buf.deinit();
        buf.appendNTimes(0, _db.pageSize) catch unreachable;
        const p = _db.pageInBuffer(buf.items, 0);
        self.meta.write(p);
        // Write the meta page to file.
        const opts = _db.opts.?;
        const sz = @as(u64, _db.pageSize);
        const writeSize = opts(_db.file, buf.items, sz) catch unreachable;
        assert(writeSize == sz, "failed to write meta page to disk, {} != {}", .{ writeSize, sz });
        if (!_db.noSync) { // TODO
            _db.file.sync() catch unreachable;
        }
        // Update statistics.
        self.stats.write += 1;
    }

    /// Internal rollback function.
    pub fn _rollback(self: *Self) void {
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
        const _db = self.db.?;
        if (self.writable) {
            // Grab freelist stats.
            const freelistFreeN = _db.freelist.freeCount();
            const freelistPendingN = _db.freelist.pendingCount();
            const freelistAlloc = _db.freelist.size();

            // Remove transaction ref & writer lock.
            _db.rwtx = null;
            _db.rwlock.unlock();

            // Merge statistics.
            _db.statlock.lock();
            _db.stats.freePageN = freelistFreeN;
            _db.stats.pendingPageN = freelistPendingN;
            _db.stats.freeAlloc = (freelistFreeN + freelistPendingN) + self.getDB().pageSize;
            _db.stats.freelistInuse = freelistAlloc;
            _db.stats.txStats.add(&self.stats);
            _db.statlock.unlock();
        } else {
            _db.removeTx(self);
            std.log.info("remove tx({}) from db", .{self.meta.txid});
        }
        std.log.info("before clear all reference", .{});
        // Clear all reference.
        self.allocator.destroy(self.meta);
        std.log.info("after clear meta", .{});
        self.db = null;
        if (self.pages) |_pages| {
            var itr = _pages.valueIterator();
            while (itr.next()) |p| {
                self.allocator.free(p.*.asSlice());
            }
            _pages.deinit();
            self.allocator.destroy(_pages);
            self.pages = null;
        }
        self.autoFreeNodes.deinit(self.allocator);
        self.root.deinit();
        std.log.info("after clear root", .{});
        // Execute commit handlers now that the locks have been removed.
        std.log.info("execute commit handlers {}", .{self._commitHandlers.capacity});
        for (self._commitHandlers.items) |func| {
            func.execute();
        }
    }

    /// Iterates over every page within a given page and executes a function.
    pub fn forEachPage(self: *Self, pgid: PgidType, depth: usize, context: anytype, comptime travel: fn (@TypeOf(context), p: *const page.Page, depth: usize) void) void {
        const p = self.getPage(pgid);
        // Execute function.
        travel(context, p, depth);
        // Recursively loop over children.
        if (p.flags & consts.intFromFlags(consts.PageFlag.branch) != 0) {
            for (0..p.count) |i| {
                const elem = p.branchPageElementPtr(i);
                self.forEachPage(elem.pgid, depth + 1, context, travel);
            }
        }
    }

    /// Returns a reference to the page with a given id.
    /// If page has been written to then a temporary buffered page is returned.
    pub fn getPage(self: *Self, id: PgidType) *page.Page {
        // Check the dirty pages first.
        if (self.pages.?.get(id)) |p| {
            std.log.debug("get page from pages cache: {}", .{id});
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

    /// Allocate a page from the database.
    pub fn allocate(self: *Self, count: usize) !*page.Page {
        const p = try self.db.?.allocatePage(count);
        // Save to our page cache.
        self.pages.?.put(p.id, p) catch unreachable;
        // Update statistics.
        self.stats.pageCount += 1;
        self.stats.pageAlloc += count * self.db.?.pageSize;
        return p;
    }
};

// Represents statusctics about the actiosn performed by the transaction
pub const TxStats = packed struct {
    // Page statistics.
    pageCount: usize = 0, // number of page allocations
    pageAlloc: usize = 0, // total bytes allocated

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
        self.pageCount += other.pageCount;
        self.pageAlloc += other.pageAlloc;
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
            .pageCount = self.pageCount - other.pageCount,
            .pageAlloc = self.pageAlloc - other.pageAlloc,
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
