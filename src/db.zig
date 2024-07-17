const std = @import("std");
const page = @import("./page.zig");
const tx = @import("./tx.zig");
const errors = @import("./error.zig");
const bucket = @import("./bucket.zig");
const freelist = @import("./freelist.zig");
const util = @import("./util.zig");
const consts = @import("./consts.zig");
const Error = @import("./error.zig").Error;

const Page = page.Page;
// TODO
const IgnoreNoSync = false;
// Page size for db is set to the OS page size.
const default_page_size = std.os.getPageSize();

pub const DB = struct {
    pageSize: usize,

    // When enabled, the database will perform a check() after every commit.
    // A painic if issued if the database is in an inconsistent stats. This
    // flag has a large performance impact so it should only be used for debugging purposes.
    strict_mode: bool,

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
    //lockFile: std.fs.File,
    dataRef: ?[]u8, // mmap'ed readonly, write throws SEGV
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
    readOnly: bool,

    meta0: *Meta,
    meta1: *Meta,

    opts: ?*const fn ([]u8, i64) void,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Return the path to currently open database file.
    pub fn path(self: *const Self) []const u8 {
        return self._path;
    }

    /// Returns the string representation of the database.
    pub fn string(self: *const Self, _allocator: std.mem.Allocator) []u8 {
        const buf = std.ArrayList(u8).init(_allocator);
        defer buf.deinit();
        const writer = buf.writer();
        writer.print("meta0: {}\n", .{self.meta0}) catch unreachable;
        writer.print("meta1: {}\n", .{self.meta1}) catch unreachable;
        writer.print("freelist:{}\n", .{self.freelist}) catch unreachable;
        writer.print("DB<{}>\n", self._path);
        return buf.toOwnedSlice();
    }

    /// Creates and opens a database at the given path.
    /// If the file does not exist then it will be created automatically.
    /// Passing in null options will cause Bolt to open the database with the default options.
    pub fn open(allocator: std.mem.Allocator, filePath: []const u8, fileMode: std.fs.File.Mode, options: Options) !*Self {
        const db = try allocator.create(DB);
        db.allocator = allocator;
        // Set default options if no options are proveide.
        db.noGrowSync = options.noGrowSync;
        db.mmapFlags = options.mmap_flags;
        // Set default values for later DB operations.
        db.maxBatchSize = consts.DefaultMaxBatchSize;
        db.maxBatchDelay = consts.DefaultMaxBatchDelay;
        db.allocSize = consts.DefaultAllocSize;
        db.mmaplock = .{};
        db.metalock = .{};
        db.statlock = .{};
        db.rwtx = null;

        // Open data file and separate sync handler for metadata writes.
        db._path = filePath;

        if (options.read_only) {
            db.readOnly = true;
            db.file = try std.fs.cwd().openFile(db._path, std.fs.File.OpenFlags{
                .lock = .shared,
                .mode = .read_only,
            });
        }
        if (!options.read_only) {
            const createFlag = std.fs.File.CreateFlags{
                .mode = fileMode,
                .truncate = false,
                .exclusive = true,
                .lock = .exclusive,
            };
            db.file = try std.fs.cwd().createFile(db._path, createFlag);
        }
        // Lock file so that other processes using Bolt in read-write mmode cannot
        // use the database at the same time. This would cause corruption since
        // the two processes would write meta pagesand free pages separately.
        // The database file is locked exclusively (only one process can grab the lock)
        // if !options.ReadOnly.
        // The database file is locked using the shared lock (more than oe process may hold a lock at the same time) otherwise (options.ReadOnly is set).
        // TODO

        // Initialize the database if it doesn't exist.
        const stat = try db.file.stat();
        if (stat.size == 0) {
            // Initialize new files with meta pagess.
            try db.init();
        } else {
            // TODO
            // Read the first meta page to determine the page size.
        }
        errdefer db.close() catch unreachable;
        // Memory map the data file.
        try db.mmap(options.initialMmapSize);
        return db;
    }

    /// init creates a new database file and initializes its meta pages.
    fn init(self: *Self) !void {
        // Set the page size to the OS page size.
        self.pageSize = consts.PageSize;
        // Create two meta pages on a buffer.
        const buf = try self.allocator.alloc(u8, self.pageSize * 4);
        for (0..2) |i| {
            const p = self.pageInBuffer(buf, @as(page.PgidType, i));
            p.id = @as(page.PgidType, i);
            p.flags = consts.intFromFlags(consts.PageFlag.meta);

            // Initialize the meta pages.
            const m = p.meta();
            m.magic = consts.Magic;
            m.version = consts.Version;
            m.page_size = @truncate(self.pageSize);
            m.free_list = 2;
            m.root = bucket._Bucket{ .root = 3 }; // So the top root bucket is a leaf
            m.pgid = 4; // 0, 1 = meta, 2 = freelist, 3 = root bucket
            m.txid = @as(consts.TxId, i);
            std.debug.print("init meta{}\n", .{i});
            m.check_sum = m.sum64();
        }

        // Write an empty freelist at page 3.
        {
            const p = self.pageInBuffer(buf, 2);
            p.id = 2;
            p.flags = consts.intFromFlags(consts.PageFlag.free_list);
            p.count = 0;
        }
        // Write an empty leaf page at page 4.
        {
            const p = self.pageInBuffer(buf, 3);
            p.id = 3;
            p.flags = consts.intFromFlags(consts.PageFlag.leaf);
            p.count = 0;
        }

        // Write the buffer to our data file.
        try self.file.pwriteAll(buf, 0);
        try self.file.sync();
    }

    /// Opens the underlying memory-mapped file and initializes the meta references.
    /// minsz is the minimum size that the new mmap can be.
    pub fn mmap(self: *Self, minsz: usize) !void {
        self.mmaplock.lock();
        defer self.mmaplock.unlock();

        const fileInfo = try self.file.metadata();
        // ensure the size is at least the minmum size.
        var size = @as(usize, fileInfo.size());
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
        _ = try util.mmap(self.file, size, true);

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
        while (i < 30) {
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
    pub fn stats(self: *const Self) Stats {
        self.statlock.lockShared();
        defer self.statlock.unlockShared();
        return self.stats;
    }

    /// Retrives a page reference from the mmap based on the current page size.
    pub fn pageById(self: *Self, id: page.PgidType) *Page {
        const pos: u64 = id * @as(u64, self.pageSize);
        const buf = self.dataRef.?[pos..self.pageSize];
        return Page.init(buf);
    }

    /// Retrives a page reference from a given byte array based on the current page size.
    pub fn pageInBuffer(self: *Self, buffer: []u8, id: page.PgidType) *page.Page {
        const pos: u64 = id * @as(u64, self.pageSize);
        const buf = buffer[pos..(pos + self.pageSize)];
        return Page.init(buf);
    }

    // meta retriews the current meta page reference.
    pub fn getMeta(self: *Self) *Meta {
        // We have to return the meta with the highest txid which does't fail
        // validation. Otherwise, we can cause errors when in fact the database is
        // in a consistent state. metaA is the one with thwe higher txid.
        var metaA = self.meta0;
        var metaB = self.meta1;

        if (self.meta1.txid > self.meta0.txid) {
            metaA = self.meta1;
            metaB = self.meta0;
        }

        // Use higher meta page if valid. Otherwise fallback to prevous, if valid.
        metaA.validate() catch |err| switch (err) {
            errors.Error => {},
            else => {
                return metaA;
            },
        };
        metaB.validate() catch |err| switch (err) {
            errors.Error => {},
            else => {
                return metaB;
            },
        };

        @panic("bolt.db.meta(): invalid meta pages");
    }

    pub fn allocatePage(self: *Self, count: usize) !*Page {
        // TODO Allocate a tempory buffer for the page.
        const buf = try self.allocator.alloc(u8, count * self.pageSize);
        const p = Page.init(buf);
        p.overflow = count - 1;

        // Use pages from the freelist if they are availiable.
        p.id = self.freelist.allocate(count);
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
        self.rwtx.?.meta.pgid += @as(page.PgidType, count);

        return p;
    }

    // Grows the size of the database to the given sz.
    pub fn grow(self: *Self, sz: usize) !void {
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

    pub fn isReadOnly(self: *const Self) bool {
        return self.readOnly;
    }

    pub fn close(self: *Self) !void {
        defer self.allocator.destroy(self);
        // self.metalock.lock();
        // defer self.metalock.unlock();
        if (self.dataRef) |data| {
            self.allocator.free(data);
        }
    }
};

// Represents the options that can be set when opening a database.
pub const Options = packed struct {
    // The amount of time to what wait to obtain a file lock.
    // When set to zero it will wait indefinitely. This option is only
    // available on Darwin and Linux.
    timeout: i64 = 0, // unit:nas

    // Sets the DB.no_grow_sync flag before money mapping the file.
    noGrowSync: bool = false,

    // Open database in read-only mode, Uses flock(..., LOCK_SH | LOCK_NB) to
    // grab a shared lock (UNIX).
    read_only: bool = false,

    // Sets the DB.mmap_flags before memory mapping the file.
    mmap_flags: isize = 0,

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

// Represents the options used if null options are passed into open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
pub const defaultOptions = Options{
    .timeout = 0,
    .noGrowSync = false,
};

// Represents statistics about the database
pub const Stats = packed struct {
    // freelist stats
    free_page_n: usize, // total number of free pages on the freelist
    pending_page_n: usize, // total number of pending pages on the freelist
    free_alloc: usize, // total bytes allocated in free pages
    free_list_inuse: usize, // total bytes used by the freelist

    // Transaction stats
    tx_n: usize, // total number of started read transactions
    open_tx_n: usize, // number of currently open read transactions

    tx_stats: tx.TxStats, // global, ongoing stats

    const Self = @This();

    pub fn sub(self: *Self, other: *Stats) Stats {
        if (other == null) {
            return self.*;
        }
        const diff = Stats{
            .free_page_n = self.free_page_n,
            .pending_page_n = self.pending_page_n,
            .free_alloc = self.free_alloc,
            .free_list_inuse = self.free_list_inuse,
            .tx_n = self.tx_n - other.tx_n,
            .tx_stats = self.tx_stats.sub(other.tx_stats),
        };
        return diff;
    }

    pub fn add(self: *Self, other: *Stats) void {
        self.tx_stats.add(other.tx_stats);
    }
};

pub const Info = packed struct {
    data: usize,
    page_size: usize,
};

pub const Meta = packed struct {
    magic: u32 = 0,
    version: u32 = 0,
    page_size: u32 = 0,
    flags: u32 = 0,
    root: bucket._Bucket = bucket._Bucket{ .root = 0, .sequence = 0 },
    free_list: page.PgidType = 0,
    pgid: page.PgidType = 0,
    txid: consts.TxId = 0,
    check_sum: u64 = 0,

    const Self = @This();
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

    // Writes the meta onto a page.
    pub fn write(self: *Self, p: *page.Page) void {
        if (self.root.root >= self.pgid) {
            unreachable;
        } else if (self.free_list >= self.pgid) {
            unreachable;
        }
        // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
        p.id = @as(page.PgidType, self.txid % 2);
        p.flags |= page.PageFlage.meta;

        // Calculate the checksum.
        self.check_sum = self.sum64();
        const meta = p.meta();
        meta.* = self.*;
        return;
    }
};

// test "meta" {
//     const stats = Info{ .data = 10, .page_size = 20 };
//     std.debug.print("{}\n", .{stats});

//     const meta = Meta{};
//     std.debug.print("{}\n", .{meta});
// }

fn opfn(p: []u8, n: i64) void {
    std.debug.print("excute me: {any}, {}\n", .{ p, n });
}

test "DB" {
    var options = defaultOptions;
    options.read_only = false;
    options.initialMmapSize = consts.PageSize;
    const filePath = try std.fmt.allocPrint(std.testing.allocator, "dirty/{}.db", .{std.time.timestamp()});
    defer std.testing.allocator.free(filePath);
    const kvDB = DB.open(std.testing.allocator, filePath, 0, options) catch unreachable;
    defer kvDB.close() catch unreachable;
}
