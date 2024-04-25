# mini-bitcask-rs
基于 bitcask 存储模型的极简 KV 磁盘存储引擎，300 行代码实现了最核心的逻辑，简单易懂，你的第一个 Rust 实战项目！通过此项目可以学习到 Rust 大多数基础知识，例如：

- 数据类型，数组、整型等
- match 表达式
- 函数
- 结构体
- 错误处理
- 迭代器 Iterator 和 DoubleEndedIterator
- 文件读写操作
- BufWriter 和 BufReader
- 单元测试撰写



**可参考资料：**

* bitcask 论文：https://riak.com/assets/bitcask-intro.pdf
* 之前写过的一篇 Go 语言实现的文章：[从零实现一个 KV 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)
* rosedb，一个生产级、更完整的 bitcask 存储模型实现：https://github.com/rosedblabs/rosedb

