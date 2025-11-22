# ameriDB 

## TODO
### Block & Page
- [x] Implement Blockd(filename, blkNum)
- [x] Implement Page backed by ByteBuffer
  - [x] getInt / setInt 
  - [x] getBytes / setBytes 
  - [x] string encodingI
  - [x] page header helpers

### StorageEngine
- [x] Implement read(BlockId, ByteBuffer)
- [x] Implement write(BlockId, ByteBuffer)
- [x] Implement append(filename)
- [x] Implement length(filename)
- [x] Ensure correct block size handling
- [x] File creation & channel management

### WAL Record Format
- [x] Define log record binary layout:
- [x] recordSize (int)
- [x] LSN (long)
- [x] payload (bytes)

### WAL Implementation
- [x] Implement append(payload) → Lsn
- [x] Implement flush(lsn)
- [x] Implement block-based storage for WAL
- [x] Maintain free-space boundary in each WAL block

### Iterator
- [x] Implement forward iterator over WAL
- [x] Decode (recordSize, LSN, payload)
- [x] Handle multi-block WAL traversal
