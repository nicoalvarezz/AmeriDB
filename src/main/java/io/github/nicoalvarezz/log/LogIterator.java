package io.github.nicoalvarezz.log;

import io.github.nicoalvarezz.file.Block;
import io.github.nicoalvarezz.file.FileManager;
import io.github.nicoalvarezz.file.Page;

import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private Block block;
    private final Page page;
    private int currentPosition;
    private int boundary;

    public LogIterator(FileManager fileManager, Block block) {
        this.fileManager = fileManager;
        this.block = block;
        byte[] bytes = new byte[fileManager.blockSize()];
        page = new Page(bytes);
        moveToBlock(block);
    }

    @Override
    public boolean hasNext() {
        return currentPosition<fileManager.blockSize() || block.number()>0;
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileManager.blockSize()) {
            block = new Block(block.fileName(), block.number() - 1);
            moveToBlock(block);
        }
        byte[] record = page.getBytes(currentPosition);
        currentPosition += Integer.BYTES + record.length;
        return record;
    }

    private void moveToBlock(Block block) {
        fileManager.read(block, page);
        boundary = page.getInt(0);
        currentPosition = boundary;
    }
}
