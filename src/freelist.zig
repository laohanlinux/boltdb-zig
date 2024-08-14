const page = @import("page.zig");
const std = @import("std");
const tx = @import("tx.zig");
const Error = @import("error.zig").Error;
const consts = @import("consts.zig");
const PgidType = page.PgidType;
const assert = @import("assert.zig").assert;
const TxId = consts.TxId;
// FreeList represents a list of all pages that are available for allcoation.
// It also tracks pages that  have been freed but are still in use by open transactions.
pub const FreeList = struct {
    // all free and available free page ids.
    ids: std.ArrayList(PgidType),
    // mapping of soon-to-be free page ids by tx.
    pending: std.AutoHashMap(consts.TxId, []page.PgidType),
    // fast lookup of all free and pending pgae ids.
    cache: std.AutoHashMap(PgidType, bool),

    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) *Self {
        const f = allocator.create(Self) catch unreachable;
        f.ids = std.ArrayList(PgidType).init(allocator);
        f.pending = std.AutoHashMap(TxId, []page.PgidType).init(allocator);
        f.cache = std.AutoHashMap(PgidType, bool).init(allocator);
        f.allocator = allocator;
        return f;
    }

    pub fn deinit(self: *Self) void {
        defer self.allocator.destroy(self);
        self.pending.deinit();
        self.cache.deinit();
        self.ids.deinit();
    }

    // Return the size of the page after serlialization.
    pub fn size(self: *Self) usize {
        var n: usize = self.count();
        if (n >= 0xFFFF) {
            // The first elements will be used to store the count. See freelist.write.
            n += 1;
        }
        return page.Page.headerSize() + @sizeOf(page.PgidType) * n;
    }

    // Returns count of pages on the freelist.
    pub fn count(self: *Self) usize {
        return self.freeCount() + self.pendingCount();
    }

    // Returns count of free pages.
    pub fn freeCount(self: *Self) usize {
        return self.ids.items.len;
    }

    // Returns count of pending pages.
    pub fn pendingCount(self: *Self) usize {
        var _count: usize = 0;
        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            _count += entry.value_ptr.len;
        }
        return _count;
    }

    // Copies into dst a list of all free ids end all pending ids in one sorted list.
    pub fn copyAll(self: *Self, dst: []page.PgidType) void {
        var array = std.ArrayList(page.PgidType).initCapacity(self.allocator, self.pendingCount()) catch unreachable;
        defer array.deinit();
        var itr = self.pending.valueIterator();
        while (itr.next()) |entries| {
            array.appendSlice(entries.*) catch unreachable;
        }
        assert(array.items.len == self.pendingCount(), "sanity check!", .{});
        Self.merge_sorted_array(dst, self.ids.items, array.items);
    }

    // Returns the starting page id of a contiguous list of pages of a given size.
    pub fn allocate(self: *Self, n: usize) page.PgidType {
        if (self.ids.items.len == 0) {
            return 0;
        }

        var initial: usize = 0;
        const previd: usize = 0;
        for (self.ids.items, 0..) |id, i| {
            assert(id > 1, "invalid page({}) allocation", .{id});

            // Reset initial page if this is not contigous.
            if (previd == 0 or id - previd != 1) {
                initial = id;
            }
            // If we found a contignous block then remove it and return it.
            if ((id - initial) + 1 == @as(page.PgidType, n)) {
                // If we're allocating off the beginning then take the fast path
                // and just adjust then existing slice. This will use extra memory
                // temporarilly but then append() in free() will realloc the slice
                // as is necessary.
                if (i + 1 == n) {
                    std.mem.copyForwards(page.PgidType, self.ids.items[0..], self.ids.items[i + 1 ..]);
                    self.ids.resize(self.ids.items.len - i - 1) catch unreachable;
                } else {
                    std.mem.copyForwards(page.PgidType, self.ids.items[i - n + 1 ..], self.ids.items[(i + 1)..]);
                    self.ids.resize(self.ids.items.len - n) catch unreachable;
                }

                // Remove from the free cache.
                // TODO Notice
                for (0..n) |ii| {
                    _ = self.cache.remove(initial + ii);
                }

                return initial;
            }
        }
        return 0;
    }

    // Releases a page and its overflow for a given transaction id.
    // If the page is already free then a panic will occur.
    pub fn free(self: *Self, txid: TxId, p: *const page.Page) !void {
        assert(p.id > 1, "can not free 0 or 1 page", .{});

        // Free page and all its overflow pages.
        const pendingIds: []u64 = self.pending.get(txid) orelse &.{};
        var ids = std.ArrayList(page.PgidType).init(std.heap.page_allocator);
        ids.appendSlice(pendingIds) catch unreachable;
        for (p.id..(p.id + p.overflow + 1)) |id| {
            // Verify that page is not already free.
            assert(!self.cache.contains(id), "page({}) already free", .{id});
            std.log.debug("free a page, id: {}", .{id});
            // Add to the freelist and cache.
            try ids.append(id);
            try self.cache.put(id, true);
        }
        if (pendingIds.len > 0) {
            self.allocator.free(pendingIds);
        }
        const _ids = try ids.toOwnedSlice();
        try self.pending.put(txid, _ids);
    }

    // Moves all page ids for a transaction id (or older) to the freelist.
    pub fn release(self: *Self, txid: TxId) !void {
        var m = std.ArrayList(page.PgidType).init(self.allocator);
        defer m.deinit();
        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            if (entry.key_ptr.* <= txid) {
                // Move transaction's pending pages to the available freelist.
                // Don't remove from the cache since the page is still free.
                try m.appendSlice(entry.value_ptr.*);
                _ = self.pending.remove(entry.key_ptr.*);
            }
        }

        const array_ids = try m.toOwnedSlice();
        std.mem.sort(page.PgidType, array_ids, {}, std.sort.asc(page.PgidType));
        var array = try std.ArrayList(page.PgidType).initCapacity(self.allocator, array_ids.len + self.ids.items.len);
        defer array.deinit();
        Self.merge_sorted_array(array.items, array_ids, self.ids.items);
        try self.ids.resize(0);
        try self.ids.appendSlice(array.items);
    }

    // Removes the pages from a given pending tx.
    pub fn rollback(self: *Self, txid: TxId) void {
        // Remove page ids from cache.
        if (self.pending.get(txid)) |pendingIds| {
            for (pendingIds) |id| {
                _ = self.cache.remove(id);
            }
        }
        // Remove pages from pending list.
        _ = self.pending.remove(txid);
    }

    // Returns whether a given page is in the free list.
    pub fn freed(self: *Self, pgid: page.PgidType) bool {
        return self.cache.contains(pgid);
    }

    /// Initializes the freelist from a freelist page.
    pub fn read(self: *Self, p: *page.Page) void {
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
            std.mem.sortUnstable(page.PgidType, self.ids.items, {}, std.sort.asc(page.PgidType));
        }
        // Rebuild the page cache.
        self.reindex();
    }

    /// Writes the page ids onto a freelist page. All free and pending ids are
    /// saved to disk since in the event of a program crash, all pending ids will
    /// become free.
    pub fn write(self: *Self, p: *page.Page) Error!void {
        defer std.log.info("after write freelist", .{});
        // Combine the old free pgids and pgids waiting on an open transaction.
        //
        // Update the header flag.
        p.flags |= consts.intFromFlags(.free_list);
        p.overflow = 0;

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

    // Reads the freelist from a page and filters out pending itmes.
    pub fn reload(self: *Self, p: *page.Page) void {
        self.read(p);

        // Build a cache of only pending pages.
        var pcache = std.AutoHashMap(page.PgidType, bool).init(self.allocator);
        var vitr = self.pending.valueIterator();

        while (vitr.next()) |pendingIDs| {
            for (pendingIDs.*) |pendingID| {
                pcache.put(pendingID, true) catch unreachable;
            }
        }

        // Check each page in the freelist and build a new available freelist.
        // with any pages not in the pending lists.
        var a = std.ArrayList(page.PgidType).init(self.allocator);
        defer a.deinit();
        for (self.ids.items) |id| {
            if (!pcache.contains(id)) {
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

        var itr = self.pending.iterator();
        while (itr.next()) |entry| {
            const pitr = entry.value_ptr.*;
            for (pitr) |id| {
                self.cache.put(id, true) catch unreachable;
            }
        }
    }

    pub fn merge_sorted_array(dst: []page.PgidType, a: []const page.PgidType, b: []const page.PgidType) void {
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
            std.mem.copyForwards(page.PgidType, dst[index..], a[i..]);
        }
        if (j < size2) {
            std.mem.copyForwards(page.PgidType, dst[index..], b[j..]);
        }
    }

    // Copies the sorted unoin of a and b into dst.
    pub fn merge_ids(dst: page.PgIds, a: page.PgIds, b: page.PgIds) void {
        if (a.len == 0) {
            std.mem.copy(dst, a);
        }
        if (b.len == 0) {
            std.mem.copy(dst, b);
        }

        // Assign lead to the slice with a lower starting value, follow to the heigher value.
        var lead = a;
        var follow = b;
        if (b[0] < a[0]) {
            lead = b;
            follow = a;
        }

        // Continue while there are elements in the lead.
        while (lead.len > 0) {
            // Merge largest prefix of lead that is ahead of follow[0].
            const n = std.sort.binarySearch(
                page.PgIds,
                follow[0],
                lead[0..],
                {},
                std.sort.asc(page.PgidType),
            );

            if (n == null) {
                break;
            }
        }
    }

    pub fn string(self: *Self, _allocator: std.mem.Allocator) []u8 {
        var buf = std.ArrayList(u8).init(_allocator);
        defer buf.deinit();
        const writer = buf.writer();
        writer.print("count: {}, freeCount: {}, pendingCount: {}", .{ self.count(), self.freeCount(), self.pendingCount() }) catch unreachable;
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
