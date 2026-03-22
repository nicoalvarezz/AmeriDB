# AmeriDB - Storage Manager

Before diving into AmeriDB and how the storage manager is built, it important to go through what the
storage manager is and what is is in charge of.

The **Storage Manager** (often referred as the **Storage Engine**) is a foundational component of a database
management system (DBMS) architecture. It operates at the lowest level of the system's storage stack,
handling physical organisation and I/O of data on non-volatile storage.

## Core definition and responsibility

- **Lowest Level**: The Storage Manager is located above the Disk Manger, and below components like Access Methods and Buffer Pool Manger.
- **Communication Layer**: It is the part of the system that communicates the hardware or the storage device, either directly through the OS or using specialised access methods.
- **Data Retrieval**: Its primary responsibility is to retrieve data and bring it into the database system's memory (the buffer pool)

## File and page management

The storage manager maintains and coordinates the various files that compromise the database.

- **File Structure**: The DBMS may maintain one or more files on disk. While system like SQLite use a single file, the case of **correspond** maintains multiple files for database tables. Each database has its own file, which correspond to its own storage manager.
- **Page Organisation**: It manages the database files, which are broke up into fixed blocks called pages.
- **Page Directory**: For systems using multiple file , the Storage Manager release on the Heap File Page directory to map unique Page ID to its physical location. However, for `AmeriDB` this is slightly different.

### Database Cluster

In the case of  `AmeriDB` I decided to go with a mulit-file database. This means that each table will have its own `.data` file. To handle this, I went with an approach very similar to postgres where we have a database cluster. This is a top-level container that manages multiple independent databases on disk.
The "cluster" is the entire data directory (`/base`) containing many databases one folder per `DatabaseId` (this would correspond to `OID` in postgres), one `database.data` file per database.

Thanks to this design we don't really need a page directory. A `PageId` is only meaningful within a specific `StorageManager` instance. It's a local identifier, not a a global one. The `DatabaseCluster` routes you to the right `StorageManage` via the `DatabaseId`, and within the manager, `PageId` identifies a page. So the full the address of any page is a really a two-part key:

```
(DatabaseId, PageId) -> physical localtion in file
```

## AmeriDB Storage Manager Data Structures

### Database Cluster

The router between our multi-file database and the different storage managers

```rust
/// Identifies a speicifc database file within the cluster.
/// Equivalent to an OID in PostgreSQL - every databse gets a unique numeric instance
/// The cluster uses this to route requests to correct StorageManager instance.
pub struct DatabaseId(pub u64); 

pub struct DatabaseCluster {
    /// Root directory of the cluster (e.g. `./data`).
    /// All database subdirectories live under `{cluster_dir}/base/{database_id}/`.
    cluster_dir: PathBuf,

    /// Default page size applied when opening a new database file.
    /// Can be overridden per-database in the future to support different block sizes.
    default_page_size: usize,

    /// Cache of open StorageManagers, keyed by DatabaseId.
    /// Wrapped in Arc<Mutex<>> so the same manager instance can be shared and mutated
    /// safely across threads. Opening the same DatabaseId twice returns the cached Arc.
    databases: Mutex<HashMap<DatabaseId, Arc<Mutex<StorageManager>>>>,
}
```

### Disk Layer

Disk layer structs to allow us to create the interface between hardware and are database. Thanks to the following data
structures we can interact with the disk and build the storage manager.

```rust

/// Default page size: 4 KiB, matching the OS virtual memory page size.
/// Fixed at compile time — all pages in a given database file share this size.
pub const PAGE_SIZE: usize = 4096;

/// Magic bytes written at the start of every database file.
/// Used on open to verify we are reading an AmeriDB file and not arbitrary data.
const DB_MAGIC: [u8; 4] = *b"AMDB";

/// File format version. Checked on open to reject files written by incompatible versions.
const DB_VERSION: u16 = 1;

/// Identifies a specific page within a single database file.
/// A PageId is local to one StorageManager — it has no meaning outside its file.
/// The full global address of a page is the two-part key (DatabaseId, PageId).
pub struct PageId(pub u64);

/// An in-memory representation of one fixed-size block on disk.
/// `data` is always exactly `page_size` bytes — the StorageManager enforces this.
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
}

/// The file header written at byte offset 0 of every database file.
/// Persisted on every structural change (e.g. after allocating a new page) so that
/// `StorageManager::open` can recover the correct state after a restart or crash.
///
/// Layout (18 bytes, little-endian):
///   [0..4]   magic       — b"AMDB", identifies this as an AmeriDB file
///   [4..6]   version     — file format version, currently 1
///   [6..10]  page_size   — size of each page in bytes
///   [10..18] next_page_id — next page ID to be handed out by allocate_pa
pub struct PageHeader {
    pub magic: [u8, 4],
    pub version: u16,
    pub page_size: u32,
    pub next_page_id: u64,
}

/// The primary interface between the DBMS and a single database file on disk.
/// Owns the file handle and is the only place that performs raw I/O.
/// All page reads and writes go through this struct.
pub struct StorageManager {
    /// Handle to the open database file.
    file: File,

    /// Path to the database file on disk. Kept for diagnostics and re-open scenarios.
    path: PathBuf,

    /// Size of each page in bytes. Read from the file header on open,
    /// so re-opening an existing file always uses the page size it was created with.
    page_size: usize,

    /// The next PageId that will be handed out by `allocate_page`.
    /// Persisted in the file header so it survives restarts.
    next_page_id: PageId,
}
```

Now, let's dive into what each of the functions of the storage manage actually work. This is one of key elements of
database and it is key to fully understand this section.

```rust
impl StorageManager {

    /// Opens (or creates) the database file at `data_file_path`.
    ///
    /// **New file**: writes an initial file header with the given `page_size` and
    /// `next_page_id = 0`, then syncs to disk so the header is durable before any
    /// pages are allocated.
    ///
    /// **Existing file**: reads and validates the file header — checks the magic bytes,
    /// the format version, and that page_size is non-zero. Recovers `next_page_id` from
    /// the header, which is how the manager knows where to resume after a restart or crash.
    ///
    /// Returns an error if:
    /// - `page_size` is 0 or exceeds u32::MAX (header cannot represent it)
    /// - the file header contains an unrecognised magic value
    /// - the file header contains an unsupported version number
    pub fn open(data_file_path: impl AsRef<Path>, page_size: usize) -> io::Result<Self>

    /// Reads the contents of the page identified by `page_id` from disk.
    ///
    /// The byte offset is computed as:
    ///   offset = PageHeader::SIZE + (page_id * page_size)
    ///
    /// Uses positioned I/O (`read_exact_at`) so the file's seek position is never
    /// touched — safe to call concurrently from multiple threads without external locking.
    pub fn read_page(&mut self, page_id: PageId) -> io::Result<Page>

    /// Writes the contents of `page` to its position on disk.
    ///
    /// The target offset is derived from `page.id` using the same formula as `read_page`.
    /// Validates that:
    /// - `page.data` is exactly `page_size` bytes (size mismatch is a caller bug)
    /// - `page.id` is less than `next_page_id` (writing to an unallocated page is rejected)
    ///
    /// Uses positioned I/O (`write_all_at`) — does not move the file cursor.
    /// Does NOT call sync after writing; the caller is responsible for durability.
    pub fn write_page(&mut self, page: &Page) -> io::Result<()>

    /// Reserves physical space for a new page and returns its PageId.
    ///
    /// Steps:
    ///   1. Computes the offset for `next_page_id` and writes `page_size` zero bytes
    ///      to extend the file — this physically reserves the space on disk.
    ///   2. Advances `next_page_id` by 1.
    ///   3. Persists the updated file header so the new `next_page_id` survives a crash.
    ///      If the header write fails, `next_page_id` is rolled back to keep the in-memory
    ///      state consistent with what is on disk.
    ///
    /// The caller must subsequently call `write_page` to populate the page with real data,
    /// and `sync_data` (or `sync_all`) to make it durable.
    pub fn allocate_page(&mut self) -> io::Result<PageId>
}
```

**Notes**:

- `AsRef<T>` is a Rust trait used for cheap, reference-to-reference conversions,
 allowing functions to accept multiple types that can be borrowed.
 For example, accepting both `String` and `&str` using `AsRef<str>`

#### Page Header

##### Why does `PageHeader` store `next_page_id`?

The short answer is _crash recovery_. Without it, the `StorageManager` has no way to know how many pages were allocated the last time the database was open.

Consider what happens without it. You open a database, allocate three pages, write data to them, and the process exits -- cleanly or otherwise. The next time you call `StorageManager::open`, the file is there, the data is there, but there is no in-memory `StorageManager` anymore. You need to reconstruct one. How do you know the next free page ID is 3 and not 0? You could scan the file and infer it from the file size, but that's fragile -- it asumes the file was never pre-allocated, never had partial write, and that file size is always reliable indicator of logical page count. None of those assumptions are safe. 

By persisting `next_page_id` in the header, `open` has single authoritative source of truth. It reads 18 bytes, validates magic, version, and immediately knows exactly where to resume. No scanning, no inference.

##### Why does `allocate_page` persist the header immediately?

This is the subtle half of the same decision. `allocate_page` doesn't just advance `next_page_id` in memory -- it writes the updated header to disk before returning. The reason is the failure window. If you allocate a page, write data to it, but the process crashes befoer he header is updated, the next `open` will recover a `next_page_id` that points to a page that already has data. Persisting the header immediately after advancing `next_page_id` closes that window.

This is also why the rollback in the `allocate_page` matters. If the header write fails `next_page_id` is restored in memory so the in-memory state never gets ahead fo what is actually on disk.

##### The trade-off
The cost is an extra `write_all_at` on every allocation. For a write-heavy workload that allocates many pages, this adds up. Real databases address this with a WAL rather than syncing the header on every allocation, they log an intent and recovery from the log. But at this stage of the project, eager header persistence is the right call: simple, correct, and easy to reason about

#### Positioned I/O

The storage manager in AmeriDB uses positioned I/O (`read_exact_at`, `write_all_at`) for all page reads and writes.

The standard lib proides `std::os::unix::fs::FileExt`, which exposes:

- `read_at` / `read_exact_at`
- `write_at` / `write_all_at`
- vectored variants

These map directly to POSIX `pread` / `pwrite` system calls (on Linux, macOS)

There are a number of benefits behind using positioned I/O for all reads and writes:

- Single syscall per page -> better random I/O performance. Avoids two syscalls (`lseek` +`read` / `write`).
- Thread-safety; no race on the file descriptor's seek positioned -> `&self` methods become safe to call concurrenlty
(only  content-level locking needed)
- Cleaner code; offset calculation is explicit and local for each call.
- Enables future patterns: concurrent writes, nmap alternative , direct I/O experiments.

```rust
impl StorageManager {

    /// Reads page data from disk using positioned I/O.
    /// `read_exact_at` maps to the POSIX `pread` syscall — a single syscall that
    /// reads at an explicit offset without moving the file cursor.
    pub fn read_page(&mut self, page_id: PageId) -> io::Result<Page> {
        let offset = self.page_offset(page_id)?;
        let mut page = Page::new(page_id, self.page_size);
        self.file.read_exact_at(&mut page.data, offset)?;
        Ok(page)
    }

    /// Writes page data to disk using positioned I/O.
    /// `write_all_at` maps to the POSIX `pwrite` syscall — same benefits as `read_exact_at`.
    /// Validates page size and that the page has been allocated before writing.
    pub fn write_page(&mut self, page: &Page) -> io::Result<()> {
        // ... size and bounds checks ...
        let offset = self.page_offset(page.id)?;
        self.file.write_all_at(&page.data, offset)?;
        Ok(())
    }
}
```

#### Syncing

In page-based databases, deciding when to call `flush()`, `sync_data()` or `sync_all()` is one of the
central durability vs performance trade-offs.

| Method             | What actually does (on Unix)                          | Cost / latency                                | Guarantees durability after call returns | Typical uses in databases                    |
| ------------------ | ----------------------------------------------------- | --------------------------------------------- | ---------------------------------------- | -------------------------------------------- |
| `file.flus()`      | Motly a no-op on `std::fs::File` (no userland butter) | Very cheap (almost free)                      | No - data still in OS page cache         | Almost never needed alone                    |
| `file.sync_data()` | `fdatasync()` -- flush data blocks to stable storage  | Medium-high (10-100x slower than plain write) | Yes (for the content you wrote)          | Most common choice for page/DB durability    |
| `file_sync_all()`  | `fsync()` --flush data + metadata (size, mtime, ect.) | Slightly higher than sync_data                | Yes (data + file attributes)             | When yo u also changed file size or metadata |

As of right now the AmeriDB StorageManager does not sync data after write operations yet. However, I have added some
helper method to allow this separately specially for test. Also it is important to start thinking about this
early on, as it is the only way to eachieve full atomicity and make sure that the DB follows ACID best practices.

In terms of AmeriDB it will most likely be a call to `file.sync_data` after every right. It is cheap enough and
we make sure the data is stored.

`sync_all` is specifically needed after `allocate_page` because that operation extends the file size — a metadata change that `sync_data` doesn't guarantee to persist.
