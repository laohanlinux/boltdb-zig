# boltdb-zig

A pure Zig implementation of BoltDB, an embedded key/value database.

## Overview

boltdb-zig is a Zig port of the original [BoltDB](https://github.com/boltdb/bolt), which is a simple yet powerful embedded key/value database written in Go. It provides a consistent and ACID-compliant data store with the following features:

- Pure Zig implementation
- Single file backend
- ACID transactions
- Lock-free MVCC
- Nested buckets

## Status

🚧 This project is currently under development.

## Usage

```zig
# zig fetch --save git+https://github.com/laohanlinux/boltdb-zig.git
```

import the library in your build.zig.zon file: [link](example/build.zig.zon)

```
const boltdbDep = b.dependency("boltdb-zig", .{
    .target = target,
    .optimize = optimize,
});

const exe = b.addExecutable(.{
    .name = "example",
    .root_source_file = b.path("src/main.zig"),
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("boltdb", boltdbDep.module("boltdb"));
```

How use?
[link](example/src/main.zig)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Original [BoltDB](https://github.com/boltdb/bolt)

