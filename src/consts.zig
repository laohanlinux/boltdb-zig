const std = @import("std");

/// Represents a marker value to indicate that a file is a Bolt DB.
pub const Magic = 0xED0CDAED;
/// The data file format verison.
pub const Version = 1;

/// The largest step that can be taken when remapping the mmap.
pub const MaxMMapStep: u64 = 1 << 30; // 1 GB

/// Default values if not set in a DB instance.
pub const DefaultMaxBatchSize = 1000;
pub const DefaultMaxBatchDelay = 10; // millisecond
pub const DefaultAllocSize = 16 * 1024 * 1024;

/// A bucket leaf flag.
pub const BucketLeafFlag: u32 = 0x01;

pub const MinFillPercent: f64 = 0.1;
pub const MaxFillPercent: f64 = 1.0;

/// The maximum length of a key, in bytes
pub const MaxKeySize: usize = 32768;
/// The maximum length of a value, in bytes
pub const MaxValueSize: usize = (1 << 32) - 2;

/// The percentage that split pages are filled.
/// This value can be changed by setting Bucket.FillPercent.
pub const DefaultFillPercent = 0.5;

/// A page flag.
pub const PageFlag = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    meta = 0x04,
    free_list = 0x10,
};
/// A bucket leaf flag.
pub const bucket_leaf_flag: u32 = 0x01;
/// A page id type.
pub const PgidType = u64;
/// A slice of page ids.
pub const PgIds = []PgidType;
/// The size of a page.
pub const PageSize: usize = std.mem.page_size;

/// Returns the size of a page given the page size and branching factor.
pub fn intFromFlags(pageFlage: PageFlag) u16 {
    return @as(u16, @intFromEnum(pageFlage));
}

/// Represents the internal transaction indentifier.
pub const TxId = u64;

/// A tuple of two elements.
pub const Tuple = struct {
    /// A tuple of two elements.
    pub fn t2(comptime firstType: type, comptime secondType: type) type {
        return struct {
            first: firstType,
            second: secondType,
        };
    }

    /// A tuple of three elements.
    pub fn t3(comptime firstType: type, comptime secondType: type, comptime thirdType: type) type {
        return struct {
            first: firstType,
            second: secondType,
            third: thirdType,
        };
    }
};

/// A key-value pair.
pub const KeyPair = struct {
    key: ?[]const u8,
    value: ?[]const u8,
    /// Create a new key-value pair.
    pub fn init(_key: ?[]const u8, _value: ?[]const u8) @This() {
        return KeyPair{ .key = _key, .value = _value };
    }
};

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

/// Get the global general purpose allocator.
pub fn getGpa() *std.heap.GeneralPurposeAllocator {
    return &gpa;
}

/// A string with reference counting.
pub const BufStr = struct {
    _str: []const u8,
    ref: *std.atomic.Value(i64),
    _allocator: ?std.mem.Allocator,

    /// Init a string.
    pub fn init(allocator: ?std.mem.Allocator, str: []const u8) @This() {
        const _allocator = allocator orelse gpa.allocator();
        const refValue = _allocator.create(std.atomic.Value(i64)) catch unreachable;
        refValue.store(1, .seq_cst);
        return .{ ._str = str, ._allocator = allocator, .ref = refValue };
    }

    /// Deinit a string.
    pub fn deinit(self: *@This()) void {
        const refValue = self.ref.fetchSub(1, .seq_cst);
        if (refValue < 1) {
            unreachable;
        }
        if (refValue == 1) {
            if (self._allocator) |allocator| {
                allocator.free(self._str);
                allocator.destroy(self.ref);
            } else {
                gpa.allocator().destroy(self.ref);
                std.debug.print("deinit\n", .{});
            }
            self.* = undefined;
        }
    }

    /// Clone a string.
    pub fn clone(self: *@This()) @This() {
        _ = self.ref.fetchAdd(1, .seq_cst);
        return .{
            ._allocator = self._allocator,
            ._str = self._str,
            .ref = self.ref,
        };
    }

    /// Get the string as a slice.
    pub fn asSlice(self: *@This()) []const u8 {
        return self._str;
    }
};

pub const Color = enum {
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,

    pub fn ansiCode(self: Color) []const u8 {
        return switch (self) {
            .Red => "\x1b[31m",
            .Green => "\x1b[32m",
            .Yellow => "\x1b[33m",
            .Blue => "\x1b[34m",
            .Magenta => "\x1b[35m",
            .Cyan => "\x1b[36m",
            .White => "\x1b[37m",
        };
    }
};

const ResetColor = "\x1b[0m";

pub const Table = struct {
    headers: std.ArrayList([]const u8),
    rows: std.ArrayList(std.ArrayList([]const u8)),
    columnWidth: usize,
    allocator: std.mem.Allocator,
    headerColor: Color,
    name: []const u8,

    pub fn init(allocator: std.mem.Allocator, columnWidth: usize, headerColor: Color, name: []const u8) @This() {
        return .{
            .headers = std.ArrayList([]const u8).init(allocator),
            .rows = std.ArrayList(std.ArrayList([]const u8)).init(allocator),
            .columnWidth = columnWidth,
            .allocator = allocator,
            .headerColor = headerColor,
            .name = name,
        };
    }

    pub fn deinit(self: *@This()) void {
        for (self.headers.items) |header| {
            self.allocator.free(header);
        }
        self.headers.deinit();
        for (self.rows.items) |row| {
            for (row.items) |cell| {
                self.allocator.free(cell);
            }
            row.deinit();
        }
        self.rows.deinit();
    }

    pub fn addHeader(self: *@This(), comptime header: anytype) !void {
        inline for (header) |cell| {
            const cp = try self.allocator.dupe(u8, cell);
            try self.headers.append(cp);
        }
    }

    pub fn addRow(self: *@This(), row: anytype) !void {
        var rowList = std.ArrayList([]const u8).init(self.allocator);
        inline for (row) |cell| {
            const cellStr = switch (@TypeOf(cell)) {
                u64, usize, i64, isize, u32, i32, u16, i16, u8, i8 => try std.fmt.allocPrint(self.allocator, "{d}", .{cell}),
                bool => try std.fmt.allocPrint(self.allocator, "{s}", .{if (cell) "true" else "false"}),
                else => try std.fmt.allocPrint(self.allocator, "{s}", .{cell}),
            };
            try rowList.append(cellStr);
        }
        try self.rows.append(rowList);
    }

    pub fn print(self: @This()) !void {
        const writer = std.io.getStdOut().writer();

        // 打印表格名称
        const totalWidth = self.columnWidth * self.headers.items.len + self.headers.items.len + 1;
        const nameLen = self.name.len;
        const leftPadding = if (totalWidth > nameLen) (totalWidth - nameLen) / 2 else 0;
        const rightPadding = if (totalWidth > nameLen + leftPadding) totalWidth - nameLen - leftPadding else 0;

        try writer.writeByteNTimes('-', leftPadding);
        try writer.print(" {s} ", .{self.name});
        try writer.writeByteNTimes('-', rightPadding);
        try writer.print("\n", .{});

        // 打印顶部分隔线
        try self.printSeparator(writer);

        // 打印表头（带颜色）
        try writer.print("{s}", .{self.headerColor.ansiCode()});
        try self.printRow(writer, self.headers.items);
        try writer.print("{s}\n", .{ResetColor});

        // 打印表头和数据之间的分隔线
        try self.printSeparator(writer);

        // 打印数据行
        for (self.rows.items) |row| {
            try self.printRow(writer, row.items);
            try writer.print("\n", .{});
        }

        // 打印底部分隔线
        try self.printSeparator(writer);
    }

    fn printSeparator(self: @This(), writer: anytype) !void {
        try writer.writeByte('+');
        for (self.headers.items) |_| {
            try writer.writeByteNTimes('-', self.columnWidth);
            try writer.writeByte('+');
        }
        try writer.print("\n", .{});
    }

    fn printRow(self: @This(), writer: anytype, row: []const []const u8) !void {
        try writer.writeByte('|');
        for (row) |cell| {
            var cellLen: usize = cell.len;
            if (cellLen > self.columnWidth) {
                cellLen = self.columnWidth;
            }
            const padding = if (cellLen < self.columnWidth) (self.columnWidth - cellLen) / 2 else 0;
            try writer.writeByteNTimes(' ', padding);
            if (cell.len > self.columnWidth) {
                try writer.print("{s}...", .{cell[0 .. self.columnWidth - 3]});
            } else {
                try writer.print("{s}", .{cell});
                try writer.writeByteNTimes(' ', self.columnWidth - cellLen - padding);
            }
            try writer.writeByte('|');
        }
    }
};

test "Table" {
    const allocator = std.testing.allocator;
    var table = Table.init(allocator, 15, .Cyan, "User Information");
    defer table.deinit();

    try table.addHeader(.{ "Name", "Age", "Gender" });

    try table.addRow(.{ "John Doe", "30", "Male" });
    try table.addRow(.{ "Jane Doe", "25", "Female" });
    try table.print();
}
