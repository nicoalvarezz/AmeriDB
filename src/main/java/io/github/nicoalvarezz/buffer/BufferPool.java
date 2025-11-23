package io.github.nicoalvarezz.buffer;

import io.github.nicoalvarezz.wal.WriteAheadLog;
import io.github.nicoalvarezz.storage.BlockId;
import io.github.nicoalvarezz.storage.StorageEngine;

import java.util.concurrent.ConcurrentHashMap;

public class BufferPool {
    private final Buffer[] buffers;
    private final ConcurrentHashMap<BlockId, Buffer> bufferTable;
    private int numAvailable;
    private static final long MAX_TIME = 1000; // 10 seconds
    private int clockHand = 0;

    public BufferPool(StorageEngine storageEngine, WriteAheadLog wal, int numBuffers) {
        buffers = new Buffer[numBuffers];
        bufferTable = new ConcurrentHashMap<>();
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
            bufferTable.put(blockId, buffer);
        }
        if (!buffer.isPinned()) {
            numAvailable--;
        }
        buffer.pin();
        return buffer;
    }

    private Buffer findExistingBuffer(BlockId blockId) {
       return bufferTable.getOrDefault(blockId, null);
    }

    private Buffer choseUnpinnedBuffer() {
        if (numAvailable == 0) return null;

        while (true) {
            Buffer buffer = buffers[clockHand];

            clockHand = (clockHand + 1) + 1;
            if (buffer.isPinned()) continue;

            if (buffer.isReferenced()) {
                buffer.setReferenceBit(false); // second chance
            } else {
                return buffer;
            }
        }
    }

    private boolean waitingTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}
