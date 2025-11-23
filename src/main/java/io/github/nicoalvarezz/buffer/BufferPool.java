package io.github.nicoalvarezz.buffer;

import io.github.nicoalvarezz.wal.WriteAheadLog;
import io.github.nicoalvarezz.storage.BlockId;
import io.github.nicoalvarezz.storage.StorageEngine;

public class BufferPool {
    private final Buffer[] buffers;
    private int numAvailable;
    private static final long MAX_TIME = 1000; // 10 seconds

    public BufferPool(StorageEngine storageEngine, WriteAheadLog wal, int numBuffers) {
        buffers = new Buffer[numBuffers];
        numAvailable = numBuffers;
        for (int i = 0; i < numBuffers; i++) buffers[i] = new Buffer(storageEngine, wal);
    }

    public synchronized int available() {
        return numAvailable;
    }

    public synchronized void flushAll(int txnum) {
        for (Buffer buffer : buffers) {
            if (buffer.modifyingTx() == txnum) {
                buffer.flush();
            }
        }
    }

    public synchronized Buffer pin(BlockId blockId) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(blockId);
            while (buffer == null && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(blockId);
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

    private Buffer tryToPin(BlockId blockId) {
        Buffer buffer = findExistingBuffer(blockId);
        if (buffer == null) {
            buffer = choseUnpinnedBuffer();
            if (buffer == null) {
                return null;
            }
            buffer.assignToBlock(blockId);
        }
        if (!buffer.isPinned()) {
            numAvailable--;
        }
        buffer.pin();
        return buffer;
    }

    private Buffer findExistingBuffer(BlockId blockId) {
        for (Buffer buffer : buffers) {
            BlockId b = buffer.block();
            if (b != null && b.equals(blockId)) {
                return buffer;
            }
        }
        return null;
    }

    private Buffer choseUnpinnedBuffer() {
        for (Buffer buffer : buffers) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}
