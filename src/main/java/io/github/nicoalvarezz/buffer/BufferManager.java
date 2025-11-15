package io.github.nicoalvarezz.buffer;

import io.github.nicoalvarezz.file.Block;
import io.github.nicoalvarezz.file.FileManager;
import io.github.nicoalvarezz.log.LogManager;

public class BufferManager {
    private final Buffer[] bufferPool;
    private int numAvailable;
    private static final long MAX_TIME = 1000; // 10 seconds

    public BufferManager(FileManager fileManager, LogManager logManager, int numBuffers) {
        bufferPool = new Buffer[numBuffers];
        numAvailable = numBuffers;

        for (int i = 0; i < numBuffers; i++) {
            bufferPool[i] = new Buffer(fileManager, logManager);
        }
    }

    public synchronized int available() {
        return numAvailable;
    }

    public synchronized void flushAll(int txnum) {
        for (Buffer buffer : bufferPool) {
            if (buffer.modifyingTx() == txnum) {
                buffer.flush();
            }
        }
    }

    public synchronized Buffer pin(Block block) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(block);
            while (buffer == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(block);
            }
            if (buffer == null) {
                throw new RuntimeException();
            }
            return buffer;
        } catch (InterruptedException ex) {
            throw new RuntimeException();
        }
    }

    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            numAvailable++;
            notifyAll();
        }
    }

    private Buffer tryToPin(Block block) {
        Buffer buffer = findExistingBuffer(block);
        if (buffer == null) {
            buffer = fifo();
            if (buffer == null) {
                return null;
            }
            buffer.assignToBlock(block);
        }
        if (!buffer.isPinned()) {
            numAvailable--;
        }
        buffer.pin();
        return buffer;
    }

    private Buffer findExistingBuffer(Block block) {
        for (Buffer buffer : bufferPool) {
            Block b = buffer.block();
            if (b != null && b.equals(block)) {
                return buffer;
            }
        }
        return null;
    }

    private Buffer choseUnpinnedBuffer() {
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }

    private Buffer fifo() {
        Buffer oldest = null;
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned()) return buffer;
            if (oldest == null || buffer.timestamp() < oldest.timestamp()) {
                oldest = buffer;
            }
        }
        return oldest;
    }

    private Buffer lru() {
        Buffer leastUsed = null;
        for (Buffer buffer : bufferPool) {
            if (buffer.isPinned()) continue;
            if (buffer.latestUsage() == 0) return buffer; // brand-new frame
            if (leastUsed == null || buffer.latestUsage() < leastUsed.latestUsage()) {
                leastUsed = buffer;
            }
        }
        return leastUsed;
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}
