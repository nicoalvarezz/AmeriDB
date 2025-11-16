package io.github.nicoalvarezz.wal;

import io.github.nicoalvarezz.buffer.Buffer;
import io.github.nicoalvarezz.buffer.BufferPool;
import io.github.nicoalvarezz.storage.BlockId;

import java.util.Iterator;

import static io.github.nicoalvarezz.storage.StorageConfig.BLOCK_SIZE;

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
        boolean hasNext = currentPosition < BLOCK_SIZE || blockId.number() > 0;
        if (!hasNext) {
            releaseBuffer();
        }
        return hasNext;
    }

    @Override
    public LogRecord next() {
        if (currentPosition == BLOCK_SIZE) {
            blockId = new BlockId(blockId.filename(), blockId.number() - 1);
            moveToBlock(blockId);
        }
        byte[] record = buffer.content().getBytes(currentPosition);
        currentPosition += Integer.BYTES + record.length;

        // TODO(nico): Fix lsn handlign
        return new SimpleLogRecord(new Lsn(0), record);
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
