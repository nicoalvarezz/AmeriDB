package io.github.nicoalvarezz.wal;

import io.github.nicoalvarezz.buffer.Buffer;
import io.github.nicoalvarezz.buffer.BufferPool;
import io.github.nicoalvarezz.storage.BlockId;

import java.util.Iterator;

class LogIterator implements Iterator<LogRecord> {
    private final BufferPool bufferPool;
    private BlockId blockId;
    private Buffer buffer = null;
    private int currentPosition;

    public LogIterator(BufferPool bufferPool, BlockId blockId) {
        this.bufferPool = bufferPool;
        this.blockId = blockId;
        moveToBlock(blockId);
    }

    @Override
    public boolean hasNext() {
        // hasNext is true if there is at least a record (LSN + payload length) in the current block
        // OR there are previous blocks to read.
        boolean hasNext = currentPosition >= (Long.BYTES + Integer.BYTES) || blockId.number() > 0;
        if (!hasNext) {
            releaseBuffer();
        }
        return hasNext;
    }

    @Override
    public LogRecord next() {
        // Check if we need to move to the previous block
        // The current position needs to be able to fit at least LSN (long) + payload_length (int)
        if (currentPosition < (Long.BYTES + Integer.BYTES) && blockId.number() > 0) {
            blockId = new BlockId(blockId.filename(), blockId.number() - 1);
            moveToBlock(blockId);
        } else if (currentPosition < (Long.BYTES + Integer.BYTES) && blockId.number() == 0) {
            // No more records in the current block and no previous blocks
            throw new java.util.NoSuchElementException();
        }

        // currentPosition points to the start of the record to be read.
        int payloadLength = buffer.content().getInt(currentPosition + Long.BYTES);
        
        // Total size of the record: LSN (long) + payload_length (int) + payload (bytes)
        int totalRecordSize = Long.BYTES + Integer.BYTES + payloadLength;
        
        // Adjust currentPosition to point to the start of the *previous* record for the next iteration
        currentPosition -= totalRecordSize;

        // Read the LSN and payload from the *current record's start position*
        // The start of the current record is `currentPosition + totalRecordSize`
        long lsnValue = buffer.content().getLong(currentPosition + totalRecordSize);
        byte[] record = buffer.content().getBytes(currentPosition + totalRecordSize + Long.BYTES);

        return new SimpleLogRecord(new Lsn(lsnValue), record);
    }

    private void moveToBlock(BlockId newBlock) {
        releaseBuffer();
        buffer = bufferPool.pin(newBlock);
        currentPosition = buffer.content().getInt(0);
    }

    private void releaseBuffer() {
        if (buffer != null) {
            bufferPool.unpin(buffer);
            buffer = null;
        }
    }
}
