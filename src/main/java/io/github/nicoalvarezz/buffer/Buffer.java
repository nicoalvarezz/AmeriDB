package io.github.nicoalvarezz.buffer;

import io.github.nicoalvarezz.wal.Lsn;
import io.github.nicoalvarezz.wal.WriteAheadLog;
import io.github.nicoalvarezz.storage.BlockId;
import io.github.nicoalvarezz.storage.StorageEngine;

import static io.github.nicoalvarezz.storage.StorageConfig.BLOCK_SIZE;

public class Buffer {
    private final StorageEngine storageEngine;
    private final WriteAheadLog wal;
    private final Page page;
    private BlockId blockId = null;
    private int pins = 0;
    private int txnum = -1;
    private Lsn lsn = null;

    public Buffer(StorageEngine storageEngine, WriteAheadLog wal) {
        this.storageEngine = storageEngine;
        this.wal = wal;
        page = new Page(BLOCK_SIZE);
    }

    public Page content() {
        return page;
    }

    public BlockId block() {
        return blockId;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0) {
            this.lsn = new Lsn(lsn);
        }
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    void assignToBlock(BlockId blockId) {
        flush();
        this.blockId = blockId;
        storageEngine.read(blockId, page.contents());
        pins = 0;
    }

    void flush() {
        if (txnum >= 0) {
            if (lsn != null) {
                wal.flush(lsn);
            }
            storageEngine.write(blockId, page.contents());
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