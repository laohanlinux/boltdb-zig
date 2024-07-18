

	## 内部结构



### 初始化时

```mermaid
block-beta
	columns 3
    doc("Pages"):3
    space down1<[" "]>(down) space
    
	block:e:3
		meta0["id-0 | meta0"]
		meta1["id-1 | meta1"]
		freelist["id-2 | freelist"]
		root_bucket["id-3 | rootbucket"]
	end
	style root_bucket fill:#d6d,stroke:#333,stroke-width:4px
```

​	Page如下

````rust
meta0:page.Page{ .id = 0, .flags = 4, .count = 0, .overflow = 0 }
meta1:page.Page{ .id = 1, .flags = 4, .count = 0, .overflow = 0 }
rootBucket:page.Page{ .id = 3, .flags = 2, .count = 0, .overflow = 0 }
freelist:page.Page{ .id = 2, .flags = 16, .count = 0, .overflow = 0 }
````