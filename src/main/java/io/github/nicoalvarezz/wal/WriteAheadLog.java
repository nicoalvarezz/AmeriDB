package io.github.nicoalvarezz.wal;

import io.github.nicoalvarezz.buffer.BufferPool;
import io.github.nicoalvarezz.buffer.Page;
import io.github.nicoalvarezz.storage.BlockId;
import io.github.nicoalvarezz.storage.StorageEngine;

import java.util.Iterator;

import static io.github.nicoalvarezz.storage.StorageConfig.BLOCK_SIZE;

public class WriteAheadLog {
    private final StorageEngine storageEngine;
    private final String logfile;
    private final Page logpage;
    private BlockId currentBlockId;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public WriteAheadLog(StorageEngine storageEngine, String logfile) {
        this.storageEngine = storageEngine;
        this.logfile = logfile;
        byte[] bytes = new byte[BLOCK_SIZE];
        this.logpage = new Page(bytes);
        int logSize = storageEngine.length(logfile);

        if (logSize == 0) {
            currentBlockId = appendNewBlock();
        } else {
            currentBlockId = new BlockId(logfile, logSize - 1);
            storageEngine.read(currentBlockId, logpage.contents());
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public Iterator<LogRecord> iterator(BufferPool bufferPool) {
        flush();
        return new LogIterator(bufferPool, currentBlockId);
    }

    public synchronized int append(byte[] logRecord) {
        int boundary = logpage.getInt(0);
        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES;

        if (boundary - bytesNeeded < Integer.BYTES) { // It doesn't fit, so move to next block
            flush();
            currentBlockId = appendNewBlock();
            boundary = logpage.getInt(0);
        }
        int recordPosition = boundary - bytesNeeded;
        logpage.setInt(0, recordPosition); // the new boundary
        latestLSN += 1;
        return latestLSN;
    }

    private BlockId appendNewBlock() {
        BlockId blockId = storageEngine.append(logfile);
        logpage.setInt(0, BLOCK_SIZE);
        storageEngine.write(blockId, logpage.contents());
        latestLSN += 1;
        return blockId;
    }

    private void flush() {
        storageEngine.write(currentBlockId, logpage.contents());
        lastSavedLSN = latestLSN;
    }
}
