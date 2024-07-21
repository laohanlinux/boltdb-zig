

## 内部结构

### 初始化时

```mermaid
block-beta
	columns 3
    doc("Disk File"):3
    space down1<["mmap"]>(down) space

	block:e:3
		meta0["id-0 | meta0"] 
    meta1["id-1 | meta1"] 
    freelist["id-2 | freelist"] --> root_bucket["id-3 | rootbucket"]
	end
	
	block:e:1
		File["file"]
	end
	style root_bucket fill:#d6d,stroke:#333,stroke-width:4px
```

Page如下

````rust
meta0:page.Page{ .id = 0, .flags = 4, .count = 0, .overflow = 0 }
meta1:page.Page{ .id = 1, .flags = 4, .count = 0, .overflow = 0 }
rootBucket:page.Page{ .id = 3, .flags = 2, .count = 0, .overflow = 0 }
freelist:page.Page{ .id = 2, .flags = 16, .count = 0, .overflow = 0 }
````



```mermaid
---
title: Page Tree
---
flowchart TD
	m0(meta0)
	m1(meta1: rootBucketPgid=3, freelistPgid=2)
	r0(((rootBucket)))
	f(freelistPgid)
	m1 --> r0
	m1 --> f

```

