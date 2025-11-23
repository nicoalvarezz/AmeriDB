package io.github.nicoalvarezz.buffer;

import io.github.nicoalvarezz.storage.BlockId;
import io.github.nicoalvarezz.storage.StorageEngine;
import io.github.nicoalvarezz.wal.Lsn;
import io.github.nicoalvarezz.wal.WriteAheadLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BufferTest {

    @Mock
    private StorageEngine storageEngine;

    @Mock
    private WriteAheadLog wal;

    private Buffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new Buffer(storageEngine, wal);
    }

    @Test
    void testInitialState() {
        assertNotNull(buffer.content());
        assertNull(buffer.block());
        assertFalse(buffer.isPinned());
        assertEquals(-1, buffer.modifyingTx());
        assertFalse(buffer.isReferenced());
    }

    @Test
    void testPinUnpin() {
        assertFalse(buffer.isPinned());
        buffer.pin();
        assertTrue(buffer.isPinned());
        buffer.pin();
        assertTrue(buffer.isPinned());
        buffer.unpin();
        assertTrue(buffer.isPinned());
        buffer.unpin();
        assertFalse(buffer.isPinned());
    }

    @Test
    void testModifiedFlagTracking() {
        assertEquals(-1, buffer.modifyingTx());
        buffer.setModified(1, 100);
        assertEquals(1, buffer.modifyingTx());
    }

    @Test
    void testReferenceBit() {
        assertFalse(buffer.isReferenced());
        buffer.pin();
        assertTrue(buffer.isReferenced());
        buffer.setReferenceBit(false);
        assertFalse(buffer.isReferenced());
        buffer.setReferenceBit(true);
        assertTrue(buffer.isReferenced());
    }

    @Test
    void testFlushWhenNotModified() {
        buffer.flush();
        verify(wal, never()).flush(any(Lsn.class));
        verify(storageEngine, never()).write(any(BlockId.class), any(ByteBuffer.class));
    }

    @Test
    void testFlushWhenModified() {
        BlockId blockId = new BlockId("testfile", 0);
        buffer.assignToBlock(blockId);
        buffer.setModified(1, 100);
        buffer.flush();

        verify(wal).flush(new Lsn(100));
        verify(storageEngine).write(eq(blockId), any(ByteBuffer.class));
        assertEquals(-1, buffer.modifyingTx());
    }
    
    @Test
    void testFlushWhenModifiedWithNegativeLsn() {
        BlockId blockId = new BlockId("testfile", 0);
        buffer.assignToBlock(blockId);
        buffer.setModified(1, -1);
        buffer.flush();
        verify(wal, never()).flush(any(Lsn.class));
        verify(storageEngine).write(eq(blockId), any(ByteBuffer.class));
        assertEquals(-1, buffer.modifyingTx());
    }


    @Test
    void testAssignToBlock() {
        BlockId blockId = new BlockId("testfile", 1);
        buffer.assignToBlock(blockId);

        assertEquals(blockId, buffer.block());
        verify(storageEngine).read(eq(blockId), any(ByteBuffer.class));
        assertFalse(buffer.isPinned());
    }

    @Test
    void testAssignToBlockWithDirtyBuffer() {
        BlockId initialBlock = new BlockId("initialfile", 0);
        buffer.assignToBlock(initialBlock);
        buffer.setModified(1, 100);

        BlockId newBlock = new BlockId("newfile", 1);
        buffer.assignToBlock(newBlock);

        // Verify that flush was called for the initial block
        verify(wal).flush(new Lsn(100));
        verify(storageEngine).write(eq(initialBlock), any(ByteBuffer.class));

        // Verify that the new block is assigned and read
        assertEquals(newBlock, buffer.block());
        verify(storageEngine).read(eq(newBlock), any(ByteBuffer.class));
        assertFalse(buffer.isPinned());
    }
}
