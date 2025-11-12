package io.github.nicoalvarezz.buffer;

import io.github.nicoalvarezz.file.Block;
import io.github.nicoalvarezz.file.FileManager;
import io.github.nicoalvarezz.file.Page;
import io.github.nicoalvarezz.log.LogManager;

public class Buffer {
    private final FileManager fileManager;
    private final LogManager logManager;
    private final Page contents;
    private Block block = null;
    private int pins = 0;
    private int txnum = -1;
    private int lsn = -1;

    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        contents = new Page(fileManager.blockSize());
    }

    public Page content() {
        return contents;
    }

    public Block block() {
        return block;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0) { this.lsn = lsn; }
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    void assignToBlock(Block block) {
        flush();
        this.block = block;
        fileManager.read(block, contents);
        pins = 0;
    }

    void flush() {
        if (txnum >= 0) {
            logManager.flush(lsn);
            fileManager.write(block, contents);
            txnum = -1;
        }
    }


    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }
}
