

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

```mermaid
flowchart TD
    nh["5，13"] --> n0["2，4<br>K&lt;5"] & nm("7，11<br>5&lt;K&lt;13") & nl["18，23<br>"]
    nl --> nc["15，17<br>13&lt;K&lt;18"] & no["20<br>18&lt;K&lt;23"] & nz["25<br>23&lt;K"]
    style nh fill:#FFD600,stroke-width:2px,stroke-dasharray: 2,color:#2962FF
    style n0 stroke-width:2px,stroke-dasharray: 0,fill:none,stroke:#424242

```

#### Find 20

 - 检索【5，13】，中间节点，fIndex=2（大于13，到了边界）
 - 检索【18，23】，中间节点，fIndex=1
 - 检索【20】，等于20，返回

#### Find 18

 - 检索【5，13】，中间节点，fIndex=2
 - 检索【18，23】，找到目标节点，返回

检索到的节点为branch，需要二级跳转，

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
