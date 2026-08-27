# mvcc

A minimal implementation of **Multi-Version Concurrency Control** (MVCC), supporting transaction commit/rollback and `set`/`get`/`delete`/`scan` operations.

**What is MVCC?** Instead of overwriting a value on every write, MVCC keeps multiple *versions* of each key, each tagged with a monotonically increasing version number. A transaction takes a version number when it begins and only ever sees versions that were already committed at that point — its "snapshot" — so readers never block writers, and repeatable reads come for free without taking locks.

**How it's implemented here, in the simplest way:**
- A single global `AtomicU64` counter hands out the next version number to each new transaction.
- Data is stored as `(raw_key, version) -> value` in one `BTreeMap`, so all versions of a key sit together in key order.
- Each transaction remembers which transaction ids were still active (uncommitted) when it began; a version is visible to it only if that version's writer isn't in that active set, and the version number isn't newer than its own.
- `get` scans a key's versions from newest to oldest and returns the first visible one; `commit` drops the transaction from the active set; `rollback` deletes everything it wrote, then drops it from the active set.
- Before writing, a transaction checks the latest version of the key — if that version isn't visible to it (i.e. a concurrent transaction touched the same key), it panics with a serialization error instead of silently overwriting.

Further reading (Chinese): [Rust 练手项目—实现 MVCC 多版本并发控制](https://mp.weixin.qq.com/s/I0AnsLowOeIUuHG5nxlaUA)
