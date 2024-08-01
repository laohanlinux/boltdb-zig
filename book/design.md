

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

## B+ Tree

![img](1*B--xjy9noLDOpIYOrcCevQ.png)	

### Find

![image-20240801130246037](1.png)

#### Find 20

 - 检索N0，中间节点，fIndex=0，即c0
 - 检索N1，中间节点，找到key=20，fIndex=2，即c2
 - 检索N5，叶子节点，且检索到20，即找到目标值

#### Find 53

 - 检索N0，中间节点，fIndex=1，即c1
 - 检索N2，中间节点，fIndex=1，即c1
 - 检索N6，叶子点，且未找到53，即未找到目标值

检索到的节点为branch，需要二级跳转，

## Insert

	### Insert 4

- 检索到N4
- N4=》4-i0，5-i1，8-i2，9-i3

### Insert 100

- 检索到N8
- N8=〉 90-i0，96-i1，99-i2，100-i3

### Insert 37

- 检索到N7

### Rebalance

### Spill

### Merge



### Delete

```mermaid
flowchart TD
    nh["5,20"] --> n0["2,3"] & nm("9,14,17") & nl["21,27,30"]
    nm --> ng["6,8"] & n6["10,13"] & nk["15,16"] & n8["18,19"]
    style nh fill:#FFD600,stroke-width:2px,stroke-dasharray: 2,color:#2962FF
    style n0 stroke-width:1px,stroke-dasharray: 0
```

- 删除分支节点20:
  - 
