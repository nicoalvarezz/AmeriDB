package io.github.nicoalvarezz.log;

import io.github.nicoalvarezz.buffer.BufferManager;
import io.github.nicoalvarezz.file.Block;
import io.github.nicoalvarezz.file.FileManager;
import io.github.nicoalvarezz.file.Page;

import java.util.Iterator;

public class LogManager {
    private final FileManager fileManager;
    private final String logfile;
    private final Page logpage;
    private Block currentBlock;
    private int latestLSN = 0;
    private int lastSavedLSN = 0;

    public LogManager(FileManager fileManager, String logfile) {
        this.fileManager = fileManager;
        this.logfile = logfile;
        byte[] bytes = new byte[fileManager.blockSize()];
        this.logpage = new Page(bytes);
        int logSize = fileManager.length(logfile);

        if (logSize == 0) {
            currentBlock = appendNewBlock();
        } else {
            currentBlock = new Block(logfile, logSize - 1);
            fileManager.read(currentBlock, logpage);
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public Iterator<byte[]> iterator(BufferManager bufferManager) {
        flush();
        return new LogIterator(fileManager, bufferManager, currentBlock);
    }

    public synchronized int append(byte[] logRecord) {
        int boundary = logpage.getInt(0);
        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES;

        if (boundary - bytesNeeded < Integer.BYTES) { // It doesn't fit, so move to next block
            flush();
            currentBlock = appendNewBlock();
            boundary = logpage.getInt(0);
        }
        int recordPosition = boundary - bytesNeeded;
        logpage.setInt(0, recordPosition); // the new boundary
        latestLSN += 1;
        return latestLSN;
    }

    private Block appendNewBlock() {
        Block block = fileManager.append(logfile);
        logpage.setInt(0, fileManager.blockSize());
        fileManager.write(block, logpage);
        latestLSN += 1;
        return block;
    }

    private void flush() {
        fileManager.write(currentBlock, logpage);
        lastSavedLSN = latestLSN;
    }
}
