package io.github.nicoalvarezz.wal;

import io.github.nicoalvarezz.buffer.BufferPool;
import io.github.nicoalvarezz.buffer.Page;
import io.github.nicoalvarezz.storage.BlockId;
import io.github.nicoalvarezz.storage.StorageEngine;

import java.util.Iterator;

import static io.github.nicoalvarezz.storage.StorageConfig.BLOCK_SIZE;

public class WriteAheadLog {
    private final StorageEngine storageEngine;
    private final String walFilename;
    private final Page page;
    private BlockId currentBlockId;
    private Lsn nextLs = new Lsn(0);
    private Lsn lastFlushed = new Lsn(0);

    public WriteAheadLog(StorageEngine storageEngine, String walFilename) {
        this.storageEngine = storageEngine;
        this.walFilename = walFilename;
        this.page = new Page(new byte[BLOCK_SIZE]);

        int numBlocks = storageEngine.length(walFilename);
        if (numBlocks == 0) {
            currentBlockId = appendNewBlock();
        } else {
            currentBlockId = new BlockId(walFilename, numBlocks - 1);
            storageEngine.read(currentBlockId, page.contents());
        }
    }

    public void flush(Lsn lsn) {
        if (lsn.compareTo(lastFlushed) > 0) {
            flush();
        }
    }

    public Iterator<LogRecord> iterator(BufferPool bufferPool) {
        flush();
        return new LogIterator(bufferPool, currentBlockId);
    }

    public synchronized Lsn append(byte[] logRecord) {
        int boundary = page.getInt(0);
        int recordSize = logRecord.length;
        // Total bytes needed: LSN (long) + payload (int length prefix + actual bytes)
        int bytesNeeded = Long.BYTES + Integer.BYTES + recordSize;

        if (boundary - bytesNeeded < Integer.BYTES) { // If it doesn't fit, so move to next block
            flush();
            currentBlockId = appendNewBlock();
            boundary = page.getInt(0);
        }

        int recordPosition = boundary - bytesNeeded;

        Lsn assigned = nextLs;
        nextLs = new Lsn(nextLs.value() + 1);

        // Write LSN, then payload
        page.setLong(recordPosition, assigned.value());
        page.setBytes(recordPosition + Long.BYTES, logRecord);
        page.setInt(0, recordPosition); // the new boundary


        return assigned;
    }

    private BlockId appendNewBlock() {
        BlockId blockId = storageEngine.append(walFilename);
        page.setInt(0, BLOCK_SIZE);
        storageEngine.write(blockId, page.contents());
        return blockId;
    }

    private void flush() {
        storageEngine.write(currentBlockId, page.contents());
        lastFlushed = nextLs;
    }
}
