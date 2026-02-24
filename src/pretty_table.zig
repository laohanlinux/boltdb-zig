const std = @import("std");

/// A color.
pub const Color = enum {
    Red,
    Green,
    Yellow,
    Blue,
    Magenta,
    Cyan,
    White,

    /// Get the ANSI code for a color.
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

/// Reset color.
pub const ResetColor = "\x1b[0m";

/// A table.
pub const Table = struct {
    headers: std.array_list.Managed([]const u8),
    rows: std.array_list.Managed(std.array_list.Managed([]const u8)),
    columnWidth: usize,
    allocator: std.mem.Allocator,
    headerColor: Color,
    name: []const u8,

    /// Init a table.
    pub fn init(allocator: std.mem.Allocator, columnWidth: usize, headerColor: Color, name: []const u8) @This() {
        return .{
            .headers = std.array_list.Managed([]const u8).init(allocator),
            .rows = std.array_list.Managed(std.array_list.Managed([]const u8)).init(allocator),
            .columnWidth = columnWidth,
            .allocator = allocator,
            .headerColor = headerColor,
            .name = name,
        };
    }

    /// Deinit a table.
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

    /// Add a header to a table.
    pub fn addHeader(self: *@This(), comptime header: anytype) !void {
        inline for (header) |cell| {
            const cp = try self.allocator.dupe(u8, cell);
            try self.headers.append(cp);
        }
    }

    /// Add a row to a table.
    pub fn addRow(self: *@This(), row: anytype) !void {
        var rowList = std.array_list.Managed([]const u8).init(self.allocator);
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

    /// Print a table.
    pub fn print(self: @This()) !void {
        const writer = std.io.getStdOut().writer();

        // calculate the total width of the table
        const totalWidth = self.columnWidth * self.headers.items.len + self.headers.items.len + 1;
        const nameLen = self.name.len;
        const leftPadding = if (totalWidth > nameLen) (totalWidth - nameLen) / 2 else 0;
        const rightPadding = if (totalWidth > nameLen + leftPadding) totalWidth - nameLen - leftPadding else 0;

        try writer.writeByteNTimes('-', leftPadding);
        try writer.print(" {s} ", .{self.name});
        try writer.writeByteNTimes('-', rightPadding);
        try writer.print("\n", .{});

        // print the top separator
        try self.printSeparator(writer);

        // print the header (with color)
        try writer.print("{s}", .{self.headerColor.ansiCode()});
        try self.printRow(writer, self.headers.items);
        try writer.print("{s}\n", .{ResetColor});

        // print the separator between the header and the data
        try self.printSeparator(writer);

        // print the data rows
        for (self.rows.items) |row| {
            try self.printRow(writer, row.items);
            try writer.print("\n", .{});
        }

        // print the bottom separator
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
