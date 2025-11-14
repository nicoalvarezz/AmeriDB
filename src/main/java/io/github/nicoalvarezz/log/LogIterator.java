package io.github.nicoalvarezz.log;

import io.github.nicoalvarezz.buffer.Buffer;
import io.github.nicoalvarezz.buffer.BufferManager;
import io.github.nicoalvarezz.file.Block;
import io.github.nicoalvarezz.file.FileManager;
import io.github.nicoalvarezz.file.Page;

import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private final BufferManager bufferManager;
    private Block block;
    private Buffer buffer = null;
    private int currentPosition;

    public LogIterator(FileManager fileManager, BufferManager bufferManager, Block block) {
        this.fileManager = fileManager;
        this.bufferManager = bufferManager;
        this.block = block;
        moveToBlock(block);
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = currentPosition < fileManager.blockSize() || block.number() > 0;
        if (!hasNext) {
            releaseBuffer();
        }
        return hasNext;
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileManager.blockSize()) {
            block = new Block(block.fileName(), block.number() - 1);
            moveToBlock(block);
        }
        byte[] record = buffer.content().getBytes(currentPosition);
        currentPosition += Integer.BYTES + record.length;

        return record;
    }

    private void moveToBlock(Block newBlock) {
        releaseBuffer();
        buffer = bufferManager.pin(newBlock);
        currentPosition = buffer.content().getInt(0);
    }

    private void releaseBuffer() {
        if (buffer != null) {
            bufferManager.unpin(buffer);
            buffer = null;
        }
    }
}
