# mini-bitcask-rs

A minimal key-value disk storage engine based on the **Bitcask** storage model. The core logic fits in ~300 lines, making it a great first hands-on Rust project. Through it you can learn most of Rust's everyday basics:

- Data types: arrays, integers, etc.
- `match` expressions
- Functions
- Structs
- Error handling
- `Iterator` and `DoubleEndedIterator`
- File I/O
- `BufWriter` and `BufReader`
- Writing unit tests

**What is Bitcask?** Instead of updating records in place, every write (`set` or `delete`) is *appended* to the end of a log file. An in-memory index (the "keydir") maps each key to the offset and length of its latest entry in the log, so writes are sequential and reads are O(1). Deleting a key appends a tombstone entry rather than removing anything immediately; stale entries and tombstones are only reclaimed later, during a **merge** (compaction) pass that rewrites the live data into a fresh file.

**How it's implemented here, in the simplest way:**
- Log entries are laid out as `key_len(4B) | value_len_or_tombstone(4B) | key | value` (a negative length marks a tombstone).
- `keydir: BTreeMap<Vec<u8>, (offset, length)>` is rebuilt in memory by scanning the log once on startup.
- `set`/`delete` append an entry and update the keydir; `get` looks up the keydir, then reads just that one slice of the file.
- `merge` walks the current keydir — i.e. only the live keys — rewrites them into a new log file, and swaps it in, dropping tombstones and superseded versions along the way.

**References:**

* Bitcask paper: https://riak.com/assets/bitcask-intro.pdf
* A previous write-up with a Go implementation: [从零实现一个 KV 存储引擎](https://mp.weixin.qq.com/s/s8s6VtqwdyjthR6EtuhnUA)
* rosedb, a production-grade, more complete Bitcask implementation: https://github.com/rosedblabs/rosedb
