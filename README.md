# AmeriDB

## Core Setup and Utility

- [ ] Set up the basic project structure
- [ ] Define global constants
- [ ] Implement data type utilities

## Foundational Layer: Storage and File Manger

- [ ] Implement a `Page` class/interface to represent the fixed-size block of data
- [ ] Implement a basic `DiskManager` class responsible for raw I/O
- [ ] Design and implement the Slotted Page Architecture:
  - [ ] Define the Page Header structure (ID, slot count, free space pointer)
  - [ ] Implement Slot Array management (adding retrieving slot entries)
  - [ ] Implement logic for storing/retrieving records based on `(page_id, slot_id)`
  - [ ] Implement free space management compaction (recognising records within the page)
- Implement Tuple/Record Design:
  - [ ] Define the Tuple Header structure, including the NULL-value bitmap
  - [ ] Implement methods to serialize/deserialize a tuple's fields to/from a byt array

## Memory Management: The Buffer Pool
- [ ] Implement the `BufferPool` class
- [ ] Initialise the Page Frames a single, large, direct-allocated `BytBuffer` to minimise GC impact
- [ ] Implement the Pinning Mechanism using a reference count for each frame
- [ ] Implement core methods:
  - [ ] `fetch_page(page_id)`: Gets a page from the buffer pool or reads it from disk
  - [ ] `unpin_page(page_id, is_dirty)`: Decrements the pin count and marks the page as dirty if modified
  - [ ] `flush_page(page_id)`: Forces a dirty page to be written to disk
- [ ] Implement the CLOCK Eviction Algorithm:
  - [ ] Implement the circular buffer and the "clock hand" pointer
  - [ ] Implement the logic to find an unpinned, unreferenced page to evict