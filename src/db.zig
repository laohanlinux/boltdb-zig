const std = @import("std");
const page = @import("page.zig");
const tx = @import("tx.zig");
const errors = @import("error.zig");
const bucket = @import("bucket.zig");
const freelist = @import("freelist.zig");
const util = @import("util.zig");
const consts = @import("consts.zig");
const Error = @import("error.zig").Error;
const PagePool = @import("gc.zig").PagePool;
const TX = tx.TX;
const PageFlag = consts.PageFlag;
const assert = util.assert;
const Table = @import("pretty_table.zig").Table;
const PgidType = consts.PgidType;

// const zoroutine = @import("zoroutine");
const Mutex = @import("mutex.zig").Mutex;

const Page = page.Page;
// TODO
const IgnoreNoSync = false;
// Page size for db is set to the OS page size.
const default_page_size = std.os.getPageSize();
// default options
const defaultOptions = consts.defaultOptions;

const log = std.log.scoped(.BoltDB);

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
    txs: std.array_list.Managed(*tx.TX),
    // the freelist is used to manage the *dirty* pages in the transaction, it only changes at writable transaction, but has only writable transaction once.
    // So we don't need to lock the freelist and it is safe by rwlock.
    freelist: *freelist.FreeList,

    stats: Stats,
    pagePool: ?*PagePool = null,

    rwlock: Mutex, // Allows only one writer a a time.
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

    /// Syncs the database file to disk.
    pub fn sync(self: *Self) Error!void {
        self.file.sync() catch |err| {
            log.err("failed to sync the database file: {any}", .{err});
            return Error.FileIOError;
        };
    }

    /// Returns the string representation of the database.
    pub fn string(self: *const Self, _allocator: std.mem.Allocator) []u8 {
        var buf = std.array_list.Managed(u8).init(_allocator);
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
        var buf = std.array_list.Managed(u8).init(_allocator);
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
    pub fn open(allocator: std.mem.Allocator, filePath: []const u8, fileMode: ?std.fs.File.Mode, options: consts.Options) !*Self {
        log.info("Start to open the database", .{});
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
        db.rwlock = Mutex{};
        db.rwtx = null;
        db.dataRef = null;
        db.pageSize = options.pageSize;
        if (db.pageSize == 0) {
            db.pageSize = consts.PageSize;
        }
        db.txs = std.array_list.Managed(*tx.TX).initCapacity(allocator, 0) catch unreachable;
        db.stats = Stats{};
        db.readOnly = options.readOnly;
        db.strictMode = options.strictMode;
        db.pagePool = null;
        db.opened = false;
        log.info("load database from path: {s}", .{filePath});
        // Open data file and separate sync handler for metadata writes.
        db._path = db.allocator.dupe(u8, filePath) catch unreachable;
        errdefer db.close() catch unreachable;

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
                        log.info("the db file already exists", .{});
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
            log.info("initialize a new db", .{});
            try db.init();
        } else {
            log.info("db size: {}", .{stat.size});
            // Read the first meta page to determine the page size.
            const buf = try db.allocator.alloc(u8, db.pageSize);
            defer db.allocator.free(buf);
            const sz = try db.file.readAll(buf[0..]);
            log.info("has load meta size: {}", .{sz});
            if (sz < Page.headerSize()) {
                return Error.Invalid;
            }
            var m: *Meta = undefined;
            if (sz < db.pageSize) {
                const _p = Page.init(buf[0..sz]);
                m = _p.meta();
            } else {
                m = db.pageInBuffer(buf[0..sz], 0).meta();
            }
            db.pageSize = blk: {
                m.validate() catch { // if the meta page is invalid, then set the page size to the default page size
                    log.warn("the meta page is invalid, set the page size to the default page size: {}", .{consts.PageSize});
                    break :blk consts.PageSize;
                };
                break :blk m.pageSize;
            };
            log.info("pageSize has change to: {}", .{db.pageSize});
        }
        // Memory map the data file.
        try db.mmap(options.initialMmapSize);

        // Read in the freelist.
        db.freelist = freelist.FreeList.init(db.allocator);
        const allocPage = db.pageById(db.getMeta().freelist);
        db.freelist.read(allocPage);
        db.opened = true;
        db.pagePool = db.allocator.create(PagePool) catch unreachable;
        db.pagePool.?.* = PagePool.init(db.allocator, db.pageSize);
        return db;
    }

    /// init creates a new database file and initializes its meta pages.
    pub fn init(self: *Self) !void {
        log.info("init a new db!", .{});
        // Set the page size to the OS page size.
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
            log.info("init meta{}", .{i});
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
        // defer log.info("succeed to mmap", .{});
        // log.err("mmap minsz: {}", .{minsz});
        self.mmaplock.lock();
        defer self.mmaplock.unlock();
        const fileInfo = try self.file.stat();
        // ensure the size is at least the minmum size.
        var size = fileInfo.size;
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
        // log.info("succeed to init data reference, size: {}", .{size});
        // Save references to the meta pages.
        self.meta0 = if (self.tryPageById(0)) |p| p.meta() else return Error.Invalid;
        self.meta1 = if (self.tryPageById(1)) |p| p.meta() else return Error.Invalid;

        // Validate the meta pages. We only return an error if both meta pages fail
        // validation, since meta0 failing validation means that it wasn't saved
        // properly -- but we can recover using meta1. And voice-versa.
        (self.meta0.validate()) catch |err0| {
            self.meta1.validate() catch {
                return err0;
            };
        };
        if (@import("builtin").is_test) {
            try self.meta0.print(self.allocator);
            try self.meta1.print(self.allocator);
        }
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
    pub fn getStats(self: *Self) Stats {
        self.statlock.lockShared();
        defer self.statlock.unlockShared();
        return self.stats;
    }

    /// tryPageById retrives a page reference from the mmap based on the current page size.
    pub fn tryPageById(self: *const Self, id: consts.PgidType) ?*Page {
        const pos: u64 = id * @as(u64, self.pageSize);
        if (self.dataRef.?.len < pos) {
            return null;
        }
        var buf: []u8 = undefined;
        if (self.dataRef.?.len < (pos + self.pageSize)) {
            std.log.warn("dataRef.len: {}, pos: {}, pageSize: {}, id: {}", .{ self.dataRef.?.len, pos, self.pageSize, id });
            // buf = self.dataRef.?[pos..self.dataRef.?.len];
            return null;
        } else {
            buf = self.dataRef.?[pos..(pos + self.pageSize)];
        }
        const p = Page.init(buf);
        return p;
    }

    /// Retrives a page reference from the mmap based on the current page size.
    /// TODO if the page is overflowed?
    pub fn pageById(self: *const Self, id: consts.PgidType) *Page {
        const pos: u64 = id * @as(u64, self.pageSize);
        assert(self.dataRef.?.len >= (pos + self.pageSize), "dataRef.len: {}, pos: {}, pageSize: {}, id: {}", .{ self.dataRef.?.len, pos, self.pageSize, id });
        const buf = self.dataRef.?[pos..(pos + self.pageSize)];
        // log.debug("==>retrive a page by pgid: {}, buf: {any}", .{ id, buf });

        const p = Page.init(buf);
        // log.debug("==>retrive a page by pgid<===", .{});
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
        var p: *Page = undefined;
        if (count == 1 and self.pagePool != null) {
            p = try self.pagePool.?.new();
        } else {
            log.warn("allocate more than one page, count: {}, pageSize: {}", .{ count, self.pageSize });
            const buf = try self.allocator.alloc(u8, count * self.pageSize);
            @memset(buf, 0);
            p = Page.init(buf);
        }
        p.overflow = @as(u32, @intCast(count)) - 1;
        // Use pages from the freelist if they are availiable.
        p.id = self.freelist.allocate(count);
        if (p.id != 0) {
            // log.debug("allocate a new page from freedlist, ptr: 0x{x}, pgid: {}, countPage:{}, overflowPage: {}, totalPageSize: {}, everyPageSize: {}", .{ p.ptrInt(), p.id, count, p.overflow, count * self.pageSize, self.pageSize });
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
        // log.debug("allow a new page (pgid={}) from mmap and update the meta page, pgid: from: {}, to: {}, flags:{} minsz: {}, datasz: {}", .{ p.id, self.rwtx.?.meta.pgid - @as(PgidType, count), self.rwtx.?.meta.pgid, p.flags, minsz, self.datasz });
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
        log.warn("grow the database from {} to {} bytes", .{ self.filesz, sz });
        self.filesz = sz;
    }

    /// isReadOnly returns true if the database was opened with read-only mode.
    pub fn isReadOnly(self: *const Self) bool {
        return self.readOnly;
    }

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
        // std.log.warn("lock rwlock!", .{});

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
        if (@import("builtin").is_test) {
            log.debug("After create a write transaction, txPtrInt: 0x{x}, meta: {any}", .{ trx.getTxPtr(), trx.root.*._b.? });
        }

        // Free any pages associated with closed read-only transactions.
        assert(self.txs.items.len <= 1, comptime "only one transaction need to be released", .{});
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
    pub fn update(self: *Self, execFn: fn (self: *TX) Error!void) Error!void {
        const execFnWithContext = struct {
            fn exec(_: void, trx: *TX) Error!void {
                return execFn(trx);
            }
        }.exec;
        return self.updateWithContext({}, execFnWithContext);
    }

    /// Executes a function within the context of a read-write managed transaction.
    pub fn updateWithContext(self: *Self, context: anytype, execFn: fn (ctx: @TypeOf(context), self: *TX) Error!void) Error!void {
        const trx = try self.begin(true);
        // const trxID = trx.getID();
        // log.info("Star a write transaction, txid: {}, meta.txid: {}, root: {}, sequence: {}, _Bucket: {any}", .{ trxID, trx.meta.txid, trx.meta.root.root, trx.meta.root.sequence, trx.root._b.? });
        // defer log.info("End a write transaction, txid: {}", .{trxID});

        // Mark as a managed tx so that the inner function cannot manually commit.
        trx.managed = true;
        // If an errors is returned from the function then rollback and return error.
        execFn(context, trx) catch |err| {
            trx.managed = false;
            trx.rollbackAndDestroy() catch unreachable;
            // log.info("after execute transaction commit handle", .{});
            return err;
        };
        trx.managed = false;
        // log.info("before commit transaction, txid: {}, metaid: {}, root: {}, sequence: {}, _Bucket: {any}", .{ trxID, trx.meta.txid, trx.meta.root.root, trx.meta.root.sequence, trx.root._b.? });
        defer trx.destroy();
        try trx.commit();
    }

    /// Executes a function within the context of a managed read-only transaction.
    /// Any error that is returned from the function is returned from the view() method.
    ///
    /// Attempting to manually rollback within the function will cause a panic.
    pub fn view(self: *Self, func: fn (self: *TX) Error!void) Error!void {
        return self.viewWithContext({}, struct {
            fn exec(_: void, trx: *TX) Error!void {
                return func(trx);
            }
        }.exec);
    }

    /// Executes a function within the context of a managed read-only transaction.
    pub fn viewWithContext(self: *Self, context: anytype, func: fn (ctx: @TypeOf(context), self: *TX) Error!void) Error!void {
        const trx = try self.begin(false);
        const trxID = trx.getID();
        if (@import("builtin").is_test) {
            log.info("Star a read-only transaction, txid: {}, meta_tx_id: {}, max_pgid: {}, root: {}, sequence: {}, _Bucket: {any}", .{ trxID, trx.meta.txid, trx.meta.pgid, trx.meta.root.root, trx.meta.root.sequence, trx.root._b.? });
            defer log.info("End a read-only transaction, txid: {}", .{trxID});
        }

        // Mark as managed tx so that the inner function cannot manually rollback.
        trx.managed = true;
        // If an error is returned from the function then pass it through.
        func(context, trx) catch |err| {
            std.log.warn("has error when execute transaction, txid: {}", .{trxID});
            trx.managed = false;
            trx.rollback() catch unreachable;
            trx.destroy();
            return err;
        };
        trx.managed = false;
        try trx.rollbackAndDestroy();
        // log.info("after execute transaction rollback handle", .{});
    }
    /// Removes a transaction from the database.
    pub fn removeTx(self: *Self, trx: *TX) void {
        // Release the read lock on the mmap.
        self.mmaplock.unlockShared();

        // Use the meta lock to restrict access to the DB object.
        self.metalock.lock();

        // std.log.info("remove tx({}) from db", .{trx.getID()});
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

    /// mustCheck runs a consistency check on the database and panics if any errors are found.
    pub fn mustCheck(self: *Self) void {
        log.info("🎲 Start to check the database consistency", .{});
        const updateFn = struct {
            fn update(_db: *DB, trx: *TX) Error!void {
                trx.check() catch |e| {
                    const tmpFilePath = DB.tempFilePath(_db.allocator);
                    defer _db.allocator.free(tmpFilePath);
                    const tmpFile = std.fs.createFileAbsolute(tmpFilePath, .{}) catch unreachable;
                    defer tmpFile.close();
                    try trx.copyFile(tmpFile);

                    log.info("\n\n", .{});
                    log.info("❌ Consistency check failed, error: {any}", .{e});
                    log.info("\n\n", .{});
                    log.info("db saved to:", .{});
                    log.info("{s}", .{tmpFilePath});
                    log.info("\n\n", .{});
                    std.process.exit(1);
                };
            }
        }.update;
        self.updateWithContext(self, updateFn) catch |err| switch (err) {
            Error.DatabaseNotOpen => return,
            else => util.panicFmt("consistency check failed, error: {}", .{err}),
        };
    }

    pub fn copyTempFile(self: *Self) Error!void {
        const filePath = Self.tempFilePath(self.allocator);
        const viewFn = struct {
            fn view(_: void, trx: *TX) Error!void {
                try trx.copyFile(filePath, std.fs.File.OpenMode.read_only);
            }
        }.view;
        try self.view({}, viewFn);
        std.log.info("db copied to: {}", .{filePath});
    }

    /// tempFilePath returns a temporary file path.
    fn tempFile(allocator: std.mem.Allocator, flags: std.fs.File.OpenFlags) std.fs.File {
        const fileName = tempFilePath(allocator);
        defer allocator.free(fileName);
        const fp = std.fs.openFileAbsolute(fileName, flags) catch unreachable;
        return fp;
    }

    fn tempFilePath(allocator: std.mem.Allocator) []const u8 {
        var random = std.Random.DefaultPrng.init(@as(u64, @intCast(std.time.microTimestamp())));
        const randomInt = random.random().uintLessThan(u32, std.math.maxInt(u32));
        const fileName = std.fmt.allocPrint(allocator, "/tmp/{d}.tmp", .{randomInt}) catch unreachable;
        return fileName;
    }

    fn _close(self: *Self) void {
        self.allocator.free(self._path);
        if (!self.opened) {
            log.info("the database is not opened", .{});
            return;
        }
        self.opened = false;

        util.munmap(self.dataRef.?);

        self.file.close();
        self.freelist.deinit();

        for (self.txs.items) |trx| {
            trx.destroy();
        }
        self.txs.deinit();
        if (self.pagePool != null) {
            self.pagePool.?.deinit();
            self.allocator.destroy(self.pagePool.?);
        }
    }

    /// opsWriteAt writes bytes to the file at the given offset.
    fn opsWriteAt(fp: std.fs.File, bytes: []const u8, offset: u64) Error!usize {
        // std.log.err("write at offset: {}, bytes: {any}", .{ offset, bytes.len });
        const writeN = fp.pwrite(bytes, offset) catch |err| {
            log.err("write at offset: {} failed, error: {any}", .{ offset, err });
            return Error.FileIOError;
        };
        return writeN;
    }
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
    pub fn sub(self: *Self, other: ?*Stats) Stats {
        if (other == null) {
            return self.*;
        }
        const diff = Stats{
            .freePageN = self.freePageN,
            .pendingPageN = self.pendingPageN,
            .freeAlloc = self.freeAlloc,
            .freelistInuse = self.freelistInuse,
            .txN = self.txN - other.?.txN,
            .txStats = self.txStats.sub(&other.?.txStats),
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
pub const Meta = struct {
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
        if (@import("builtin").is_test) {
            assert(p.id == 0 or p.id == 1, "page id should be 0 or 1, but got {}", .{p.id});
            assert(std.mem.eql(u8, p.typ(), "meta"), "page: {} type should be meta, but got {s}", .{ p.id, p.typ() });
        }
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

// // Ensure that opening a database with a bad path returns an error.
// test "DB-Open_ErrNotExists" {
//     std.testing.log_level = .err;
//     const badPath = "bad-path/db.db";
//     _ = DB.open(std.testing.allocator, badPath, null, defaultOptions) catch |err| {
//         std.debug.assert(err == std.fs.File.OpenError.NotDir);
//     };
// }
