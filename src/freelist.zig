const page = @import("page.zig");
const std = @import("std");
const tx = @import("tx.zig");
const Error = @import("error.zig").Error;
const consts = @import("consts.zig");
const PgidType = consts.PgidType;
const assert = @import("assert.zig").assert;
const TxId = consts.TxId;
const Page = page.Page;
// FreeList represents a list of all pages that are available for allcoation.
// It also tracks pages that  have been freed but are still in use by open transactions.
pub const FreeList = struct {
    // all free and available free page ids.
    ids: std.ArrayList(PgidType),
    // mapping of soon-to-be free page ids by tx.
    pending: std.AutoHashMap(consts.TxId, std.ArrayList(PgidType)),
    // fast lookup of all free and pending pgae ids.
    cache: std.AutoHashMap(PgidType, bool),

    allocator: std.mem.Allocator,

    const Self = @This();

    /// init freelist
    pub fn init(allocator: std.mem.Allocator) *Self {
        const f = allocator.create(Self) catch unreachable;
        f.ids = std.ArrayList(PgidType).init(allocator);
        f.pending = std.AutoHashMap(TxId, std.ArrayList(PgidType)).init(allocator);
        f.cache = std.AutoHashMap(PgidType, bool).init(allocator);
        f.allocator = allocator;
        return f;
    }

    /// deinit freelist
    pub fn deinit(self: *Self) void {
        std.log.info("deinit freelist", .{});
        defer self.allocator.destroy(self);
        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            std.log.info("free pending, txid: {}, ids: {any}", .{ entry.key_ptr.*, entry.value_ptr.items });
            entry.value_ptr.deinit();
        }
        self.pending.deinit();
        self.cache.deinit();
        self.ids.deinit();
    }

    /// Return the size of the page after serlialization.
    pub fn size(self: *Self) usize {
        var n: usize = self.count();
        if (n >= 0xFFFF) {
            // The first elements will be used to store the count. See freelist.write.
            n += 1;
        }
        return Page.headerSize() + @sizeOf(PgidType) * n;
    }

    /// Returns count of pages on the freelist.
    pub fn count(self: *Self) usize {
        return self.freeCount() + self.pendingCount();
    }

    /// Returns count of free pages.
    pub fn freeCount(self: *Self) usize {
        return self.ids.items.len;
    }

    /// Returns count of pending pages.
    pub fn pendingCount(self: *Self) usize {
        var pageCount: usize = 0;
        var itr = self.pending.valueIterator();
        while (itr.next()) |valuePtr| {
            pageCount += valuePtr.items.len;
        }
        return pageCount;
    }

    /// Copies into dst a list of all free ids end all pending ids in one sorted list.
    pub fn copyAll(self: *Self, dst: []PgidType) void {
        var array = std.ArrayList(PgidType).initCapacity(self.allocator, self.pendingCount()) catch unreachable;
        defer array.deinit();
        var itr = self.pending.valueIterator();
        while (itr.next()) |entries| {
            array.appendSlice(entries.items) catch unreachable;
        }
        assert(array.items.len == self.pendingCount(), "sanity check!", .{});
        Self.mergeSortedArray(dst, self.ids.items, array.items);
    }

    /// Returns the starting page id of a contiguous list of pages of a given size.
    pub fn allocate(self: *Self, n: usize) PgidType {
        if (self.ids.items.len == 0) {
            return 0;
        }

        var initial: usize = 0;
        const previd: usize = 0;
        std.log.debug("1, allocate n: {d}, ids: {any}", .{ n, self.ids.items });
        for (self.ids.items, 0..) |id, i| {
            assert(id > 1, "invalid page({}) allocation", .{id});

            // Reset initial page if this is not contigous.
            if (previd == 0 or (id - previd) != 1) {
                initial = id;
            }
            // If we found a contignous block then remove it and return it.
            if ((id - initial) + 1 == @as(PgidType, n)) {
                const beforeCount = self.ids.items.len;
                // If we're allocating off the beginning then take the fast path
                // and just adjust then existing slice. This will use extra memory
                // temporarilly but then append() in free() will realloc the slice
                // as is necessary.
                if (i + 1 == n) {
                    std.mem.copyForwards(PgidType, self.ids.items[0..], self.ids.items[i + 1 ..]);
                    self.ids.resize(self.ids.items.len - i - 1) catch unreachable;
                } else {
                    std.mem.copyForwards(PgidType, self.ids.items[i - n + 1 ..], self.ids.items[(i + 1)..]);
                    self.ids.resize(self.ids.items.len - n) catch unreachable;
                }

                assert(beforeCount == (n + self.ids.items.len), "beforeCount == n + self.ids.items.len, beforeCount: {d}, n: {d}, self.ids.items.len: {d}", .{ beforeCount, n, self.ids.items.len });

                // Remove from the free cache.
                // TODO Notice
                for (0..n) |ii| {
                    const have = self.cache.remove(initial + ii);
                    assert(have, "page {} not found in cache", .{initial + ii});
                }
                const afterCount = self.ids.items.len;
                assert(beforeCount == (n + afterCount), "{} != {}", .{ beforeCount, afterCount });
                std.log.debug("2, allocate a new page, pgid: {d}, n: {d}, initial: {d}", .{ initial, n, initial });
                return initial;
            }
        }
        return 0;
    }

    /// Releases a page and its overflow for a given transaction id.
    /// If the page is already free then a panic will occur.
    pub fn free(self: *Self, txid: TxId, p: *const Page) !void {
        assert(p.id > 1, "can not free 0 or 1 page", .{});
        // Free page and all its overflow pages.
        const ids = try self.pending.getOrPutValue(txid, std.ArrayList(PgidType).init(self.allocator));
        for (p.id..(p.id + p.overflow + 1)) |id| {
            // Verify that page is not already free.
            assert(!self.cache.contains(id), "page({}) already free", .{id});
            std.log.debug("free a page, id: {}", .{id});
            // Add to the freelist and cache.
            try self.cache.putNoClobber(id, true);
            try ids.value_ptr.append(id);
        }
    }

    /// Moves all page ids for a transaction id (or older) to the freelist.
    pub fn release(self: *Self, txid: TxId) !void {
        var arrayIDs = std.ArrayList(PgidType).init(self.allocator);
        defer arrayIDs.deinit();
        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            if (entry.key_ptr.* <= txid) {
                // Move transaction's pending pages to the available freelist.
                // Don't remove from the cache since the page is still free.
                try arrayIDs.appendSlice(entry.value_ptr.items);
                entry.value_ptr.deinit();
                const have = self.pending.remove(entry.key_ptr.*);
                assert(have, "sanity check", .{});
            }
        }
        // Sort the array
        std.mem.sort(PgidType, arrayIDs.items, {}, std.sort.asc(PgidType));
        var array = try std.ArrayList(PgidType).initCapacity(self.allocator, arrayIDs.items.len + self.ids.items.len);
        defer array.deinit();
        try array.appendNTimes(0, arrayIDs.items.len + self.ids.items.len);
        assert(array.items.len == (arrayIDs.items.len + self.ids.items.len), "array.items.len == (arrayIDs.items.len + self.ids.items.len)", .{});
        std.log.info("Release a tx's pages, before merge:\t {any} <= [{any}, {any}]", .{ array.items, arrayIDs.items, self.ids.items });
        Self.mergeSortedArray(array.items, arrayIDs.items, self.ids.items);
        try self.ids.resize(0);
        try self.ids.appendSlice(array.items);
        assert(self.ids.items.len == array.items.len, "self.ids.items.len == array.items.len", .{});
        std.log.info("Release a tx's pages, after merge:\t {any}", .{self.ids.items});
    }

    /// Removes the pages from a given pending tx.
    pub fn rollback(self: *Self, txid: TxId) void {
        // Remove page ids from cache.
        if (self.pending.get(txid)) |pendingIds| {
            for (pendingIds.items) |id| {
                _ = self.cache.remove(id);
            }
            pendingIds.deinit();
            // Remove pages from pending list.
            _ = self.pending.remove(txid);
        }
    }

    /// Returns whether a given page is in the free list.
    pub fn freed(self: *Self, pgid: PgidType) bool {
        return self.cache.contains(pgid);
    }

    /// Initializes the freelist from a freelist page.
    pub fn read(self: *Self, p: *Page) void {
        // If the page.count is at the max u16 value (64k) then it's considered
        // an overflow and the size of the freelist is stored as the first elment.
        var _count = @as(usize, p.count);
        var idx: usize = 0;
        if (_count == 0xFFFF) {
            idx = 1;
            _count = p.freelistPageOverWithCountElements().?[0];
        }

        // Copy the list of page ids from the freelist.
        if (_count == 0) {
            self.ids.resize(0) catch unreachable;
        } else {
            const ids = p.freelistPageOverWithCountElements().?;
            self.ids.appendSlice(ids[idx.._count]) catch unreachable;
            // Make sure they're sorted
            std.mem.sortUnstable(PgidType, self.ids.items, {}, std.sort.asc(PgidType));
        }
        // Rebuild the page cache.
        self.reindex();
    }

    /// Writes the page ids onto a freelist page. All free and pending ids are
    /// saved to disk since in the event of a program crash, all pending ids will
    /// become free.
    pub fn write(self: *Self, p: *Page) Error!void {
        defer std.log.info("after write freelist to page: {}", .{p.id});
        // Combine the old free pgids and pgids waiting on an open transaction.
        //
        // Update the header flag.
        p.flags |= consts.intFromFlags(.freeList);

        // The page.Count can only hold up to 64k elements so if we overflow that
        // number then we handle it by putting the size in the first element.
        const lenids = self.count();
        if (lenids == 0) {
            p.count = @as(u16, @intCast(lenids));
        } else if (lenids < 0xFFFF) {
            p.count = @as(u16, @intCast(lenids));
            self.copyAll(p.freelistPageElements().?);
        } else {
            p.count = @as(u16, 0xFFFF);
            const overflow = p.freelistPageOverWithCountElements().?;
            overflow[0] = @as(u64, lenids);
            p.overflow = @as(u32, @intCast(lenids));
            self.copyAll(overflow[1..]);
        }
    }

    /// Reads the freelist from a page and filters out pending itmes.
    pub fn reload(self: *Self, p: *Page) void {
        self.read(p);

        // Build a cache of only pending pages.
        var pagaeCahe = std.AutoHashMap(PgidType, bool).init(self.allocator);
        var vitr = self.pending.valueIterator();

        while (vitr.next()) |pendingIDs| {
            for (pendingIDs.items) |pendingID| {
                pagaeCahe.put(pendingID, true) catch unreachable;
            }
        }

        // Check each page in the freelist and build a new available freelist.
        // with any pages not in the pending lists.
        var a = std.ArrayList(PgidType).init(self.allocator);
        defer a.deinit();
        for (self.ids.items) |id| {
            if (!pagaeCahe.contains(id)) {
                a.append(id) catch unreachable;
            }
        }

        self.ids.appendSlice(a.items) catch unreachable;

        // Once the available list is rebuilt then rebuild the free cache so that
        // it includes the available and pending free pages.
        self.reindex();
    }

    // Rebuilds the free cache based on available and pending free list.
    fn reindex(self: *Self) void {
        self.cache.clearAndFree();
        for (self.ids.items) |id| {
            self.cache.put(id, true) catch unreachable;
        }

        var itr = self.pending.valueIterator();
        while (itr.next()) |entry| {
            for (entry.items) |id| {
                self.cache.put(id, true) catch unreachable;
            }
        }
    }

    /// Merge two sorted arrays into a third array.
    fn mergeSortedArray(dst: []PgidType, a: []const PgidType, b: []const PgidType) void {
        const size1 = a.len;
        const size2 = b.len;
        var i: usize = 0;
        var j: usize = 0;
        var index: usize = 0;

        while (i < size1 and j < size2) {
            if (a[i] <= b[j]) {
                dst[index] = a[i];
                i += 1;
            } else {
                dst[index] = b[j];
                j += 1;
            }
            index += 1;
        }
        if (i < size1) {
            std.mem.copyForwards(PgidType, dst[index..], a[i..]);
        }
        if (j < size2) {
            std.mem.copyForwards(PgidType, dst[index..], b[j..]);
        }
    }

    /// Format freelist to string with _allocator.
    pub fn string(self: *Self, _allocator: std.mem.Allocator) []u8 {
        var buf = std.ArrayList(u8).init(_allocator);
        defer buf.deinit();
        const writer = buf.writer();

        {
            writer.print("pending:", .{}) catch unreachable;
            var itr = self.pending.iterator();
            while (itr.next()) |entry| {
                writer.print(" [txid: {any}, pages: {any}], ", .{ entry.key_ptr.*, entry.value_ptr.items }) catch unreachable;
            }
            writer.print("\n", .{}) catch unreachable;
        }
        const pendingIds = self.pending.keyIterator().items;
        writer.print("count: {}, freeCount: {}, pendingCount: {}, pendingKeys: {any}", .{ self.count(), self.freeCount(), self.pendingCount(), pendingIds[0..] }) catch unreachable;
        return buf.toOwnedSlice() catch unreachable;
    }
};

// test "meta" {
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){}; // instantiate allocator
//     const galloc = gpa.allocator(); // retrieves the created allocator.
//     var ff = FreeList.init(galloc);
//     defer ff.deinit();
//     _ = ff.size();
//     _ = ff.count();
//     const fCount = ff.freeCount();
//     _ = ff.pendingCount();
//     ff.copyAll(&.{});
//     const i = ff.allocate(100);
//     try ff.release(1);
//     ff.rollback(1);
//     _ = ff.freed(200);
//     //  ff.reload(20);
//     ff.reindex();
//     try ff.cache.put(1000, true);
//     std.debug.print("What the fuck {d} {d}, {?}\n", .{ fCount, i, ff.cache.getKey(1000) });

//     const a = [_]page.PgidType{ 1, 3, 4, 5 };
//     const b = [_]page.PgidType{ 0, 2, 6, 7, 120 };
//     var array = [_]page.PgidType{0} ** (a.len + b.len);
//     FreeList.merge_sorted_array(array[0..], a[0..], b[0..]);
//     std.debug.print("after merge!\n", .{});
//     for (array) |n| {
//         std.debug.print("{},", .{n});
//     }
//     std.debug.print("\n", .{});
//     var arr = try std.ArrayList(page.PgidType).initCapacity(std.heap.page_allocator, 100);
//     defer arr.deinit();
// }

// test "freelist" {
//     var flist = FreeList.init(std.testing.allocator);
//     defer flist.deinit();

//     var ids = std.ArrayList(page.PgidType).initCapacity(std.testing.allocator, 0) catch unreachable;
//     for (0..29) |i| {
//         const pid = @as(u64, i);
//         ids.append(pid) catch unreachable;
//     }
//     defer ids.deinit();
//     std.mem.copyForwards(u64, ids.items[0..20], ids.items[10..12]);
//     ids.resize(2) catch unreachable;
//     std.debug.print("{any}\n", .{ids.items});
// }
