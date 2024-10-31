const std = @import("std");
const page = @import("page.zig");
const tx = @import("tx.zig");
const errors = @import("error.zig");
const bucket = @import("bucket.zig");
const freelist = @import("freelist.zig");
const util = @import("util.zig");
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const TX = tx.TX;
const PageFlag = consts.PageFlag;
const assert = util.assert;
const Table = consts.Table;
const PgidType = consts.PgidType;

const Page = page.Page;
// TODO
const IgnoreNoSync = false;
// Page size for db is set to the OS page size.
const default_page_size = std.os.getPageSize();

/// DB is the main struct that holds the database state.
pub const DB = struct {
    pageSize: usize,

    // When enabled, the database will perform a check() after every commit.
    // A painic if issued if the database is in an inconsistent stats. This
    // flag has a large performance impact so it should only be used for debugging purposes.
    strictMode: bool = false,

    // Setting the no_sync flag will cause the database to skip fsync()
    // calls after each commit. This can be useful when bulk loading data
    // into a database and you can restart the bulk load in the event of
    // a system failure or database corruption. Do not set this flag for
    // normal use.
    //
    // If the package global IgnoreNoSync constant is true, this value is
    // ignored.  See the comment on that constant for more details.
    //
    // THIS IS UNSAFE. PLEASE USE WITH CAUTION.
    noSync: bool = false,

    // When true, skips the truncate call when growing the database.
    // Setting this to true is only safe on non-ext3/ext4 systems.
    // Skipping truncation avoids pareallocation of hard drive space and
    // bypasssing a truncate() and fsync() syscall on remapping.
    //
    noGrowSync: bool,

    // If you want to read the entire database fast. you can set MMAPFLAG to
    // syscall.MAP_POPULATE on linux 2.6.23+ for sequential read-ahead.
    mmapFlags: isize,

    // MaxBatchSize is the maximum size of a batch. Default value is
    // copied from DefaultMaxBatchSize in open.
    //
    // If <=0, disables batching.
    //
    // Do not change concurrently with calls to Batch.
    maxBatchSize: isize,

    // MaxBatchDelay is the maximum delay before a batch starts.
    // Default value is copied from DefaultMaxBatchDelay in open.
    //
    // If <= 0, effectively disable batching.
    //
    // Do not change currently with calls to Batch,
    maxBatchDelay: isize, // millis

    // AllocSize is the amount of space allocated when the database
    // needs to create new pages. This is done to amortize the cost
    // of truncate() and fsync() when growing the data file.
    allocSize: usize,

    _path: []const u8,
    file: std.fs.File,
    filesz: usize,
    opened: bool,
    //lockFile: std.fs.File,
    dataRef: ?[]u8 = null, // mmap'ed readonly, write throws SEGV
    // data: ?*[consts.MaxMMapStep]u8,
    datasz: usize,

    rwtx: ?*tx.TX = null,
    txs: std.ArrayList(*tx.TX),
    freelist: *freelist.FreeList,
    stats: Stats,

    rwlock: std.Thread.Mutex, // Allows only one writer a a time.
    metalock: std.Thread.Mutex, // Protects meta page access.
    mmaplock: std.Thread.RwLock, // Protects mmap access during remapping.
    statlock: std.Thread.RwLock, // Protects stats access.

    // Read only mode.
    // When true, Update() and Begin(true) return Error.DatabaseReadOnly.
    readOnly: bool = false,

    meta0: *Meta,
    meta1: *Meta,

    opts: ?*const fn (std.fs.File, []const u8, u64) Error!usize,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Return the path to currently open database file.
    pub fn path(self: *const Self) []const u8 {
        return self._path;
    }

    /// Returns the string representation of the database.
    pub fn string(self: *const Self, _allocator: std.mem.Allocator) []u8 {
        var buf = std.ArrayList(u8).init(_allocator);
        defer buf.deinit();
        const writer = buf.writer();
        writer.print("meta0: {}\n", .{self.meta0.*}) catch unreachable;
        writer.print("meta1: {}\n", .{self.meta1.*}) catch unreachable;
        const str = self.freelist.string(self.allocator);
        defer self.allocator.free(str);
        writer.print("freelist:<{s}>\n", .{str}) catch unreachable;
        writer.print("DB:<{s}>\n", .{self._path}) catch unreachable;
        return buf.toOwnedSlice() catch unreachable;
    }

    /// Returns db pretty format string.
    pub fn pageString(self: *const Self, _allocator: std.mem.Allocator) []u8 {
        var buf = std.ArrayList(u8).init(_allocator);
        defer buf.deinit();
        const writer = buf.writer();
        writer.print("meta0:{}\n", .{self.pageById(0).*}) catch unreachable;
        writer.print("meta1:{}\n", .{self.pageById(1).*}) catch unreachable;
        const m = self.getMeta();
        writer.print("rootBucket:{}\n", .{self.pageById(m.root.root).*}) catch unreachable;
        writer.print("freelist:{}\n", .{self.pageById(m.freelist).*}) catch unreachable;
        return buf.toOwnedSlice() catch unreachable;
    }

    /// Creates and opens a database at the given path.
    /// If the file does not exist then it will be created automatically.
    /// Passing in null options will cause Bolt to open the database with the default options.
    pub fn open(allocator: std.mem.Allocator, filePath: []const u8, fileMode: ?std.fs.File.Mode, options: Options) !*Self {
        const db = try allocator.create(DB);
        db.allocator = allocator;
        // Set default options if no options are proveide.
        db.noGrowSync = options.noGrowSync;
        db.mmapFlags = options.mmapFlags;
        // Set default values for later DB operations.
        db.maxBatchSize = consts.DefaultMaxBatchSize;
        db.maxBatchDelay = consts.DefaultMaxBatchDelay;
        db.allocSize = consts.DefaultAllocSize;
        db.mmaplock = .{};
        db.metalock = .{};
        db.statlock = .{};
        db.rwlock = .{};
        db.rwtx = null;
        db.dataRef = null;
        db.pageSize = 0;
        db.txs = std.ArrayList(*TX).init(allocator);
        db.stats = Stats{};
        db.readOnly = options.readOnly;
        db.strictMode = options.strictMode;

        // Open data file and separate sync handler for metadata writes.
        db._path = db.allocator.dupe(u8, filePath) catch unreachable;
        if (options.readOnly) {
            db.file = try std.fs.cwd().openFile(db._path, std.fs.File.OpenFlags{
                .lock = .shared,
                .mode = .read_only,
            });
        }
        if (!options.readOnly) {
            const createFlag = std.fs.File.CreateFlags{
                .mode = fileMode orelse std.fs.File.default_mode,
                .truncate = false,
                .exclusive = true,
                .lock = .exclusive,
                .read = true,
            };

            db.file = blk: {
                const fileFp = std.fs.cwd().createFile(db._path, createFlag) catch |err| switch (err) {
                    std.fs.File.OpenError.PathAlreadyExists => {
                        const fp = try std.fs.cwd().openFile(db._path, .{ .mode = .read_write });
                        break :blk fp;
                    },
                    else => {
                        return err;
                    },
                };
                break :blk fileFp;
            };
        }
        // Lock file so that other processes using Bolt in read-write mmode cannot
        // use the database at the same time. This would cause corruption since
        // the two processes would write meta pagesand free pages separately.
        // The database file is locked exclusively (only one process can grab the lock)
        // if !options.ReadOnly.
        // The database file is locked using the shared lock (more than oe process may hold a lock at the same time) otherwise (options.ReadOnly is set).
        // TODO

        // Default values for test hooks.
        db.opts = Self.opsWriteAt;

        // Initialize the database if it doesn't exist.
        const stat = try db.file.stat();
        if (stat.size == 0) {
            // Initialize new files with meta pagess.
            try db.init();
        } else {
            // Read the first meta page to determine the page size.
            const buf = try db.allocator.alloc(u8, 0x1000);
            defer db.allocator.free(buf);
            const sz = try db.file.readAll(buf[0..]);
            std.log.info("has load meta size: {}", .{sz});
            const m = db.pageInBuffer(buf[0..sz], 0).meta();
            db.pageSize = blk: {
                m.validate() catch {
                    break :blk consts.PageSize;
                };
                break :blk m.pageSize;
            };
        }
        errdefer db.close() catch unreachable;
        // Memory map the data file.
        try db.mmap(options.initialMmapSize);

        // Read in the freelist.
        db.freelist = freelist.FreeList.init(db.allocator);
        const allocPage = db.pageById(db.getMeta().freelist);
        db.freelist.read(allocPage);
        db.opened = true;
        return db;
    }

    /// init creates a new database file and initializes its meta pages.
    pub fn init(self: *Self) !void {
        std.log.info("init a new db!", .{});
        // Set the page size to the OS page size.
        self.pageSize = consts.PageSize;
        // Create two meta pages on a buffer, and
        const buf = try self.allocator.alloc(u8, self.pageSize * 4);
        defer self.allocator.free(buf);
        for (0..2) |i| {
            const p = self.pageInBuffer(buf, @as(PgidType, i));
            p.id = @as(PgidType, i);
            p.flags = consts.intFromFlags(consts.PageFlag.meta);
            p.overflow = 0;
            p.count = 0;

            // Initialize the meta pages.
            const m = p.meta();
            m.magic = consts.Magic;
            m.version = consts.Version;
            m.pageSize = @truncate(self.pageSize);
            m.freelist = 2;
            m.root = bucket._Bucket{ .root = 3 }; // So the top root bucket is a leaf
            m.pgid = 4; // 0, 1 = meta, 2 = freelist, 3 = root bucket
            m.txid = @as(consts.TxId, i);
            m.flags = consts.intFromFlags(PageFlag.meta);
            std.log.info("init meta{}", .{i});
            m.check_sum = m.sum64();
        }

        // Write an empty freelist at page 3.
        {
            const p = self.pageInBuffer(buf, 2);
            p.id = 2;
            p.flags = consts.intFromFlags(consts.PageFlag.freeList);
            p.count = 0;
            p.overflow = 0;
        }
        // Write an empty leaf page at page 4.
        {
            const p = self.pageInBuffer(buf, 3);
            p.id = 3;
            p.flags = consts.intFromFlags(consts.PageFlag.leaf);
            p.count = 0;
            p.overflow = 0;
        }

        // Write the buffer to our data file.
        try self.file.pwriteAll(buf, 0);
        try self.file.sync();
        self.filesz = buf.len;
    }

    /// Opens the underlying memory-mapped file and initializes the meta references.
    /// minsz is the minimum size that the new mmap can be.
    pub fn mmap(self: *Self, minsz: usize) !void {
        std.log.info("mmap minsz: {}", .{minsz});
        self.mmaplock.lock();
        defer self.mmaplock.unlock();
        const fileInfo = try self.file.metadata();
        // ensure the size is at least the minmum size.
        var size = @as(usize, fileInfo.size());
        // TODO Delete it
        // assert(size >= minsz, "the size of file is less than the minsz: {d}, file size: {d}", .{ minsz, size });
        if (size < minsz) {
            size = minsz;
        }

        size = try self.mmapSize(size);

        // Dereference call mmap references before unmmapping.
        if (self.rwtx) |_rwtx| {
            _rwtx.root.dereference();
        }

        // Unmap existing data before continuing.
        if (self.dataRef) |ref| {
            util.munmap(ref);
            self.dataRef = null;
            self.datasz = 0;
        }

        // Memory-map the data file as a byte slice.
        self.dataRef = try util.mmap(self.file, size, true);
        self.datasz = size;
        assert(self.dataRef.?.len == size, "the size of dataRef is not equal to the size: {d}, dataRef.len: {d}", .{ size, self.dataRef.?.len });
        std.log.info("succeed to init data reference, size: {}", .{size});
        // Save references to the meta pages.
        self.meta0 = self.pageById(0).meta();
        self.meta1 = self.pageById(1).meta();

        // Validate the meta pages. We only return an error if both meta pages fail
        // validation, since meta0 failing validation means that it wasn't saved
        // properly -- but we can recover using meta1. And voice-versa.
        (self.meta0.validate()) catch |err0| {
            self.meta1.validate() catch {
                return err0;
            };
        };
    }

    /// Determines the appropriate size for the mmap given the current size
    /// of the database. The minmum size is 32KB and doubles until it reaches 1GB.
    /// Returns an error if the new mmap size is greater than the max allowed.
    fn mmapSize(_: *const Self, size: usize) !usize {
        // Double the size from 32KB until 1GB
        var i: u32 = 15;
        while (i <= 30) {
            const shifted = i << 1;
            if (size <= shifted) {
                return shifted;
            }
            i += 1;
        }
        const _maxMapSize = util.maxMapSize();

        // Verify the requested size is not above the maximum allowed.
        if (size > _maxMapSize) {
            return errors.Error.MMapTooLarge;
        }

        // If large than 1GB then grow by 1GB at a time.
        var sz = @as(u64, size);
        if (sz > consts.MaxMMapStep) {
            sz += (consts.MaxMMapStep - sz % consts.MaxMMapStep);
        }

        // If we've exeeded the max size then only grow up to the max size.
        if (sz > @as(u64, _maxMapSize)) {
            sz = @as(u64, _maxMapSize);
        }

        return @as(usize, sz);
    }

    /// Retrieves ongoing performance stats for the database.
    /// This is only updated when a transaction closes.
    pub fn getStats(self: *const Self) Stats {
        self.statlock.lockShared();
        defer self.statlock.unlockShared();
        return self.stats;
    }

    /// Retrives a page reference from the mmap based on the current page size.
    /// TODO if the page is overflowed?
    pub fn pageById(self: *const Self, id: consts.PgidType) *Page {
        const pos: u64 = id * @as(u64, self.pageSize);
        assert(self.dataRef.?.len >= (pos + self.pageSize), "dataRef.len: {}, pos: {}, pageSize: {}, id: {}", .{ self.dataRef.?.len, pos, self.pageSize, id });
        const buf = self.dataRef.?[pos..(pos + self.pageSize)];
        const p = Page.init(buf);
        std.log.debug("retrive a page by pgid: {}, pageSize: {}, count: {}, overflow: {}", .{ id, self.pageSize, p.count, p.overflow });
        return p;
    }

    /// Retrives a page reference from a given byte array based on the current page size.
    pub fn pageInBuffer(self: *Self, buffer: []u8, id: consts.PgidType) *page.Page {
        const pos: u64 = id * @as(u64, self.pageSize);
        const buf = buffer[pos..(pos + self.pageSize)];
        return Page.init(buf);
    }

    /// meta retriews the current meta page reference.
    pub fn getMeta(self: *const Self) *Meta {
        // We have to return the meta with the highest txid which does't fail
        // validation. Otherwise, we can cause errors when in fact the database is
        // in a consistent state. metaA is the one with thwe higher txid.
        var metaA = self.meta0;
        var metaB = self.meta1;
        // std.log.debug("meta0: {}, meta1: {}", .{ metaA.txid, metaB.txid });

        if (self.meta1.txid > self.meta0.txid) {
            metaA = self.meta1;
            metaB = self.meta0;
        }

        // Use higher meta page if valid. Otherwise fallback to prevous, if valid.
        const maxMeta = blk: {
            _ = metaA.validate() catch {
                _ = metaB.validate() catch unreachable;
                break :blk metaB;
            };
            break :blk metaA;
        };
        return maxMeta;
    }

    /// Returns the freelist associated with the database.
    pub fn getFreelist(self: *const Self) *freelist.FreeList {
        return self.freelist;
    }

    /// Allocates a count pages
    pub fn allocatePage(self: *Self, count: usize) !*Page {
        // TODO Allocate a tempory buffer for the page.
        // TODO Use PageAllocator.
        // const buf = try self.allocator.alloc(u8, count * self.pageSize);
        const buf = try self.allocator.alignedAlloc(u8, @alignOf(Page), count * self.pageSize);
        @memset(buf, 0);
        const p = Page.init(buf);
        p.overflow = @as(u32, @intCast(count)) - 1;
        // Use pages from the freelist if they are availiable.
        p.id = self.freelist.allocate(count);
        defer std.log.debug("allocate a new page, ptr: 0x{x}, pgid: {}, countPage:{}, overflowPage: {}, totalPageSize: {}, everyPageSize: {}", .{ p.ptrInt(), p.id, count, p.overflow, count * self.pageSize, self.pageSize });
        if (p.id != 0) {
            return p;
        }

        // Resize mmap() if we're at the end.
        p.id = self.rwtx.?.meta.pgid;
        const minsz: usize = (@as(usize, @intCast(p.id)) + 1 + count) * self.pageSize;
        if (minsz >= self.datasz) {
            try self.mmap(minsz);
        }

        // Move the page id high water mark.
        self.rwtx.?.meta.pgid += @as(PgidType, count);
        std.log.debug("update the meta page, pgid: {}, flags:{} minsz: {}, datasz: {}", .{ self.rwtx.?.meta.pgid, p.flags, minsz, self.datasz });
        return p;
    }

    /// Grows the size of the database to the given sz.
    pub fn grow(self: *Self, _sz: usize) !void {
        var sz = _sz;
        // Ignore if the new size is less than valiable file size.
        if (sz <= self.filesz) {
            return;
        }

        // If the data is smaller than the alloc size then only allocate what's need.
        // Once it goes over the allocation size then allocate in chunks.
        if (self.datasz < self.allocSize) {
            sz = self.datasz;
        } else {
            sz += self.allocSize;
        }

        // Truncate and fsync to ensure file size metadata is flushed.
        // https://github.com/boltdb/bolt/issues/284
        if (!self.noGrowSync and !self.readOnly) {
            if (util.isLinux()) {
                _ = std.os.linux.fsync(self.file.handle);
            }
        }

        self.filesz = sz;
    }

    /// isReadOnly returns true if the database was opened with read-only mode.
    pub fn isReadOnly(self: *const Self) bool {
        return self.readOnly;
    }

    /// close closes the database and releases all associated resources.
    pub fn close(self: *Self) !void {
        defer std.log.info("succeed to close db!", .{});
        defer self.allocator.destroy(self);
        self.rwlock.lock();
        defer self.rwlock.unlock();

        self.metalock.lock();
        defer self.metalock.unlock();

        self.mmaplock.lock();
        self.mmaplock.unlock();
        self._close();
    }

    // Begin starts a new transaction.
    // Multiple read-only transactions can be used concurrently but only one write transaction can be used at a time. Starting multiple write transactions
    // will cause the calls to back and be serialized until the current write transaction finishes.
    //
    // Transactions should not be dependent on the one another. Opening a read
    // transaction and a write transaction in the same goroutine can cause the
    // writer to deadlock because the databases periodically needs to re-map itself
    // as it grows and it cannot do that while a read transaction is open.
    //
    // If a long running read transaction (for example, a snapshot transaction) is
    // needed, you might want to send DB.initialMmapSize to a larger enough value to avoid potential blocking of write transaction.
    //
    // *IMPORTANT*: You must close read-only transactions after you are finished or else the database will not reclaim old pages.
    pub fn begin(self: *Self, writable: bool) Error!*TX {
        if (writable) {
            return self.beginRWTx();
        }
        return self.beginTx();
    }

    fn beginTx(self: *Self) Error!*TX {
        // Lock the meta pages while we initialize the transaction. We obtain
        // the meta lock before the mmap lock because that's the order that the
        // write transaction will obtain them.
        self.metalock.lock();

        // Obtain a read-only lock on the mmap. When the mmap is remapped it will
        // obtain a write lock so all transactions must finish before it can be
        // remapped.
        self.mmaplock.lockShared();

        // Exit if the database is not open yet.
        if (!self.opened) {
            self.mmaplock.unlockShared();
            self.metalock.unlock();
            return Error.DatabaseNotOpen;
        }

        // Create a transaction associated with the database.
        const trx = TX.init(self, false);
        // Keep track of transaction until it closes.
        self.txs.append(trx) catch unreachable;
        const n = self.txs.items.len;

        // Unlock the meta pages
        self.metalock.unlock();

        // Update the transaction stats.
        self.statlock.lock();
        self.stats.txN += 1;
        self.stats.openTxN = n;
        self.statlock.unlock();

        return trx;
    }

    // beginRWTx starts a new read-write transaction.
    fn beginRWTx(self: *Self) Error!*TX {
        // If the database was opened with Options.ReadOnly, return an error.
        if (self.readOnly) {
            return Error.DatabaseReadOnly;
        }

        // Obtain writer lock. This released by the transaction when it closes.
        // This is enforces only one writer transaction at a time.
        self.rwlock.lock();
        // std.log.debug("lock rwlock!", .{});

        // Once we have the writer lock then we can lock the meta pages so that
        // we can set up the transaction.
        self.metalock.lock();
        defer self.metalock.unlock();

        // Exit if the database is not open yet.
        if (!self.opened) {
            self.rwlock.unlock();
            return Error.DatabaseNotOpen;
        }

        // Create a transaction associated with the database.
        const trx = TX.init(self, true);
        trx.writable = true;
        self.rwtx = trx;
        std.log.debug("After create a write transaction, meta: {any}", .{trx.root.*._b.?});

        // Free any pages associated with closed read-only transactions.
        var minid: u64 = std.math.maxInt(u64);
        for (self.txs.items) |_trx| {
            if (_trx.meta.txid < minid) {
                minid = _trx.meta.txid;
            }
        }

        // Release the pages associated with closed read-only transactions.
        if (minid > 0) {
            self.freelist.release(minid - 1) catch unreachable;
        }

        return trx;
    }

    /// Executes a function within the context of a read-write managed transaction.
    /// If no error is returned from the function then the transaction is committed.
    /// If an error is returned then the entire transaction is rolled back.
    /// Any error that is returned from the function or returned from the commit is
    /// returned from the update() method.
    ///
    /// Attempting to manually commit or rollback within the function will cause a panic.
    pub fn update(self: *Self, context: anytype, execFn: fn (ctx: @TypeOf(context), self: *TX) Error!void) Error!void {
        const trx = try self.begin(true);
        const trxID = trx.getID();
        std.log.info("Star a write transaction, txid: {}, metaid: {}, root: {}, sequence: {}, _Bucket: {any}", .{ trxID, trx.meta.txid, trx.meta.root.root, trx.meta.root.sequence, trx.root._b.? });
        defer std.log.info("End a write transaction, txid: {}", .{trxID});
        // Make sure the transaction rolls back in the event of a panic.
        // errdefer if (trx.db != null) {
        //     trx._rollback();
        // };

        // Mark as a managed tx so that the inner function cannot manually commit.
        trx.managed = true;

        // If an errors is returned from the function then rollback and return error.
        execFn(context, trx) catch |err| {
            trx.managed = false;
            trx.rollback() catch {};
            std.log.info("after execute transaction commit handle", .{});
            return err;
        };
        trx.managed = false;
        std.log.info("before commit transaction, txid: {}, metaid: {}, root: {}, sequence: {}, _Bucket: {any}", .{ trxID, trx.meta.txid, trx.meta.root.root, trx.meta.root.sequence, trx.root._b.? });
        defer trx.destroy();
        try trx.commit();
    }

    /// Executes a function within the context of a managed read-only transaction.
    /// Any error that is returned from the function is returned from the view() method.
    ///
    /// Attempting to manually rollback within the function will cause a panic.
    pub fn view(self: *Self, context: anytype, func: fn (ctx: @TypeOf(context), self: *TX) Error!void) Error!void {
        const trx = try self.begin(false);
        const trxID = trx.getID();
        std.log.info("Star a read-only transaction, txid: {}, meta_tx_id: {}, max_pgid: {}, root: {}, sequence: {}, _Bucket: {any}", .{ trxID, trx.meta.txid, trx.meta.pgid, trx.meta.root.root, trx.meta.root.sequence, trx.root._b.? });
        defer std.log.info("End a read-only transaction, txid: {}", .{trxID});
        var isDrop = false;
        // Make sure the transaction rolls back in the event of a panic.
        defer {
            if (!isDrop and trx.db != null) {
                trx._rollback();
            }
        }

        // Mark as managed tx so that the inner function cannot manually rollback.
        trx.managed = true;

        // If an error is returned from the function then pass it through.
        func(context, trx) catch |err| {
            trx.managed = false;
            trx.rollback() catch {};
            isDrop = true;
            std.log.info("after execute transaction commit handle", .{});
            return err;
        };
        trx.managed = false;
        defer {
            isDrop = true;
        }
        try trx.rollback();
    }

    /// Removes a transaction from the database.
    pub fn removeTx(self: *Self, trx: *TX) void {
        // Release the read lock on the mmap.
        self.mmaplock.unlockShared();

        // Use the meta lock to restrict access to the DB object.
        self.metalock.lock();

        std.log.info("transaction executes rollback!", .{});
        // Remove the transaction.
        for (self.txs.items, 0..) |_trx, i| {
            if (_trx == trx) {
                _ = self.txs.orderedRemove(i);
            }
        }

        const n = self.txs.items.len;

        // Unlock the meta pages.
        self.metalock.unlock();

        // Merge statistics.
        self.statlock.lock();
        self.stats.openTxN = n;
        self.stats.txStats.add(&trx.stats);
        self.statlock.unlock();
    }

    fn _close(self: *Self) void {
        if (!self.opened) {
            return;
        }
        self.opened = false;

        util.munmap(self.dataRef.?);

        self.file.close();
        self.allocator.free(self._path);

        self.freelist.deinit();

        for (self.txs.items) |trx| {
            trx.destroy();
        }
        self.txs.deinit();
    }

    fn opsWriteAt(fp: std.fs.File, bytes: []const u8, offset: u64) Error!usize {
        const writeN = fp.pwrite(bytes, offset) catch unreachable;
        return writeN;
    }
};

/// Represents the options that can be set when opening a database.
pub const Options = packed struct {
    // The amount of time to what wait to obtain a file lock.
    // When set to zero it will wait indefinitely. This option is only
    // available on Darwin and Linux.
    timeout: i64 = 0, // unit:nas

    // Sets the DB.no_grow_sync flag before money mapping the file.
    noGrowSync: bool = false,

    // Open database in read-only mode, Uses flock(..., LOCK_SH | LOCK_NB) to
    // grab a shared lock (UNIX).
    readOnly: bool = false,

    // Sets the DB.strict_mode flag before memory mapping the file.
    strictMode: bool = false,

    // Sets the DB.mmap_flags before memory mapping the file.
    mmapFlags: isize = 0,

    // The initial mmap size of the database
    // in bytes. Read transactions won't block write transaction
    // if the initial_mmap_size is large enough to hold database mmap
    // size. (See DB.begin for more information)
    //
    // If <= 0, the initial map size is 0.
    // If initial_mmap_size is smaller than the previous database size.
    // it takes no effect.
    initialMmapSize: usize = 0,
};

/// Represents the options used if null options are passed into open().
/// No timeout is used which will cause Bolt to wait indefinitely for a lock.
pub const defaultOptions = Options{
    .timeout = 0,
    .noGrowSync = false,
};

/// Represents statistics about the database
pub const Stats = packed struct {
    // freelist stats
    freePageN: usize = 0, // total number of free pages on the freelist
    pendingPageN: usize = 0, // total number of pending pages on the freelist
    freeAlloc: usize = 0, // total bytes allocated in free pages
    freelistInuse: usize = 0, // total bytes used by the freelist

    // Transaction stats
    txN: usize = 0, // total number of started read transactions
    openTxN: usize = 0, // number of currently open read transactions

    txStats: tx.TxStats = tx.TxStats{}, // global, ongoing stats

    const Self = @This();

    /// Subtracts the statistics of one Stats from another.
    pub fn sub(self: *Self, other: *Stats) Stats {
        if (other == null) {
            return self.*;
        }
        const diff = Stats{
            .freePageN = self.freePageN,
            .pendingPageN = self.pendingPageN,
            .freeAlloc = self.freeAlloc,
            .freelistInuse = self.freelistInuse,
            .txN = self.txN - other.txN,
            .txStats = self.txStats.sub(other.txStats),
        };
        return diff;
    }

    /// Adds the statistics of one Stats to another.
    pub fn add(self: *Self, other: *Stats) void {
        self.txStats.add(other.txStats);
    }
};

pub const Info = packed struct {
    data: usize,
    page_size: usize,
};

/// Represents the meta data of the database.
pub const Meta = packed struct {
    magic: u32 = 0,
    version: u32 = 0,
    pageSize: u32 = 0,
    flags: u32 = 0,
    // the root bucket
    root: bucket._Bucket = bucket._Bucket{ .root = 0, .sequence = 0 },
    // the freelist page id
    freelist: consts.PgidType = 0,
    // the max page id
    pgid: consts.PgidType = 0,
    // the transaction id
    txid: consts.TxId = 0,
    // the checksum
    check_sum: u64 = 0,

    const Self = @This();
    // The size of the meta object.
    pub const header_size = @sizeOf(Meta);

    /// Validates the meta object.
    pub fn validate(self: *const Self) errors.Error!void {
        if (self.magic != consts.Magic) {
            return errors.Error.Invalid;
        } else if (self.version != consts.Version) {
            return errors.Error.VersionMismatch;
        } else if (self.check_sum != 0 and self.check_sum != self.sum64()) {
            return errors.Error.CheckSum;
        }
        return;
    }

    /// Calculates the checksum of the meta object
    pub fn sum64(self: *const Self) u64 {
        const endPos = @offsetOf(Self, "check_sum");
        const ptr = @intFromPtr(self);
        const buf: [*]u8 = @ptrFromInt(ptr);
        const sumBytes = buf[0..][0..endPos];
        const crc32 = std.hash.Crc32.hash(sumBytes);
        return @as(u64, crc32);
    }

    /// Copies one meta object to another
    pub fn copy(self: *Self, dest: *Self) void {
        dest.* = self.*;
    }

    /// Writes the meta onto a page.
    pub fn write(self: *Self, p: *page.Page) void {
        assert(self.root.root < self.pgid, "root page id is invalid, self's pgid: {}", .{self.pgid});
        assert(self.freelist < self.pgid, "freelist page id is invalid, self's pgid: {}", .{self.pgid});
        // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
        p.id = @as(PgidType, self.txid & 0b1);
        p.flags |= consts.intFromFlags(consts.PageFlag.meta);
        // Calculate the checksum.
        self.check_sum = self.sum64();
        const meta = p.meta();
        meta.* = self.*;
        assert(meta.check_sum == p.meta().check_sum, "CheckSum is invalid", .{});
        return;
    }

    /// Print the meta information
    pub fn print(self: *const Self, allocator: std.mem.Allocator) !void {
        var table = Table.init(allocator, 20, .Blue, "Meta");
        defer table.deinit();
        try table.addHeader(.{ "Field", "Value" });
        try table.addRow(.{ "Megic", self.magic });
        try table.addRow(.{ "Version", self.version });
        try table.addRow(.{ "Page Size", self.pageSize });
        try table.addRow(.{ "Flags", self.flags });
        try table.addRow(.{ "Root", self.root.root });
        try table.addRow(.{ "Sequence", self.root.sequence });
        try table.addRow(.{ "Freelist", self.freelist });
        try table.addRow(.{ "Pgid", self.pgid });
        try table.addRow(.{ "Txid", self.txid });
        try table.addRow(.{ "CheckSum", self.check_sum });
        try table.print();
    }
};

// test "meta" {
//     const stats = Info{ .data = 10, .page_size = 20 };
//     std.debug.print("{}\n", .{stats});

//     const meta = Meta{};
//     std.debug.print("{}\n", .{meta});
// }

// test "DB-Read" {
//     var options = defaultOptions;
//     options.read_only = false;
//     options.initialMmapSize = 10 * consts.PageSize;
//     const filePath = try std.fmt.allocPrint(std.testing.allocator, "dirty/{}.db", .{std.time.timestamp()});
//     defer std.testing.allocator.free(filePath);
//     {
//         const kvDB = DB.open(std.testing.allocator, filePath, null, options) catch unreachable;
//         try kvDB.close();
//     }
//     {
//         const kvDB = DB.open(std.testing.allocator, filePath, null, options) catch unreachable;
//         const dbStr = kvDB.string(std.testing.allocator);
//         defer std.testing.allocator.free(dbStr);
//         std.debug.print("String: {s}\n", .{dbStr});
//
//         const pageStr = kvDB.pageString(std.testing.allocator);
//         defer std.testing.allocator.free(pageStr);
//         std.debug.print("{s}\n", .{pageStr});
//         defer kvDB.close() catch unreachable;
//         {
//             const viewFn = struct {
//                 fn view(_kvDB: ?*DB, _: *TX) Error!void {
//                     if (_kvDB == null) {
//                         return Error.DatabaseNotOpen;
//                     }
//                     std.debug.print("page count: {}\n", .{_kvDB.?.pageSize});
//                 }
//             };
//             for (0..10) |i| {
//                 try kvDB.view(?*DB, kvDB, viewFn.view);
//                 std.debug.assert(kvDB.stats.tx_n == (i + 1));
//                 if (i == 9) {
//                     const err = kvDB.view(?*DB, null, viewFn.view);
//                     std.debug.assert(err == Error.DatabaseNotOpen);
//                 }
//             }
//         }
//
//         // parallel read
//         {
//             var joins = std.ArrayList(std.Thread).init(std.testing.allocator);
//             defer joins.deinit();
//             const parallelViewFn = struct {
//                 fn view(_kdb: *DB) void {
//                     const viewFn = struct {
//                         fn view(_kvDB: ?*DB, _: *TX) Error!void {
//                             if (_kvDB == null) {
//                                 return Error.DatabaseNotOpen;
//                             }
//                             std.debug.print("tid: {}\n", .{std.Thread.getCurrentId()});
//                         }
//                     };
//                     _kdb.view(?*DB, _kdb, viewFn.view) catch unreachable;
//                 }
//             };
//             for (0..10) |_| {
//                 const sp = try std.Thread.spawn(.{}, parallelViewFn.view, .{kvDB});
//                 try joins.append(sp);
//             }
//
//             for (joins.items) |join| {
//                 join.join();
//             }
//
//             std.debug.print("after stats count: {}\n", .{kvDB.readOnly});
//             std.debug.assert(kvDB.stats.tx_n == 21);
//         }
//
//         // update
//     }
// }

// test "DB-Write" {
//     std.testing.log_level = .debug;
//     var options = defaultOptions;
//     options.readOnly = false;
//     options.initialMmapSize = 10 * consts.PageSize;
//     // options.strictMode = true;
//     const filePath = try std.fmt.allocPrint(std.testing.allocator, "dirty/{}.db", .{std.time.timestamp()});
//     defer std.testing.allocator.free(filePath);

//     const kvDB = DB.open(std.testing.allocator, filePath, null, options) catch unreachable;

//     const updateFn = struct {
//         fn update(_kvDB: ?*DB, trx: *TX) Error!void {
//             if (_kvDB == null) {
//                 return Error.DatabaseNotOpen;
//             }
//             const Context = struct {
//                 _tx: *TX,
//             };
//             const ctx = Context{ ._tx = trx };
//             const forEach = struct {
//                 fn inner(_: Context, bucketName: []const u8, _: ?*bucket.Bucket) Error!void {
//                     std.log.info("execute forEach, bucket name: {s}", .{bucketName});
//                 }
//             };
//             std.log.info("Execute write transaction: {}", .{trx.getID()});
//             return trx.forEach(ctx, forEach.inner);
//         }
//     }.update;
//     {
//         // test "DB-update"
//         for (0..1) |i| {
//             _ = i; // autofix
//             try kvDB.update(kvDB, updateFn);
//             const meta = kvDB.getMeta();
//             // because only freelist page is used, so the max pgid is 5
//             assert(meta.pgid == 5, "the max pgid is invalid: {}", .{meta.pgid});
//             assert((meta.freelist == 2 or meta.freelist == 4), "the freelist is invalid: {}", .{meta.freelist});
//             assert(meta.root.root == 3, "the root is invalid: {}", .{meta.root.root});
//         }

//         // Create a bucket
//         const updateFn2 = struct {
//             fn update(_: void, trx: *TX) Error!void {
//                 var buf: [1000]usize = undefined;
//                 randomBuf(buf[0..]);
//                 std.log.info("random: {any}\n", .{buf});
//                 for (buf) |i| {
//                     const bucketName = std.fmt.allocPrint(std.testing.allocator, "hello-{d}", .{i}) catch unreachable;
//                     defer std.testing.allocator.free(bucketName);
//                     const bt = try trx.createBucket(bucketName);
//                     _ = bt; // autofix
//                 }

//                 const forEachCtx = struct {
//                     _tx: *TX,
//                 };
//                 const ctx = forEachCtx{ ._tx = trx };
//                 const forEachFn = struct {
//                     fn inner(_: forEachCtx, bucketName: []const u8, _: ?*bucket.Bucket) Error!void {
//                         std.log.info("execute forEach, bucket name: {s}", .{bucketName});
//                     }
//                 };
//                 try trx.forEach(ctx, forEachFn.inner);

//                 const bt = trx.getBucket("hello-0") orelse unreachable;
//                 try bt.put(consts.KeyPair.init("ping", "pong"));
//                 try bt.put(consts.KeyPair.init("echo", "ok"));
//                 try bt.put(consts.KeyPair.init("Alice", "Bod"));
//                 const got = bt.get("ping");
//                 std.debug.assert(got != null);
//                 std.debug.assert(std.mem.eql(u8, got.?, "pong"));

//                 const got2 = bt.get("not");
//                 std.debug.assert(got2 == null);
//             }
//         }.update;
//         try kvDB.update({}, updateFn2);

//         const viewFn = struct {
//             fn view(_: void, trx: *TX) Error!void {
//                 for (0..100) |_| {
//                     const bt = trx.getBucket("Alice");
//                     assert(bt == null, "the bucket is not null", .{});
//                 }
//                 for (0..100) |_| {
//                     const bt = trx.getBucket("hello-0").?;
//                     const kv = bt.get("not");
//                     assert(kv == null, "should be not found the key", .{});
//                 }
//             }
//         };
//         try kvDB.view({}, viewFn.view);
//         kvDB.close() catch unreachable;
//     }
//     // test "DB-read" {
//     {}
// }
