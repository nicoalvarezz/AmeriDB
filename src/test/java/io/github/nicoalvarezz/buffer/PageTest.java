package io.github.nicoalvarezz.buffer;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PageTest {

    @Test
    void readsAndWritesIntsAtVariousOffsets() {
        Page page = new Page(128);
        page.setInt(0, 42);
        page.setInt(32, 99);

        assertEquals(42, page.getInt(0));
        assertEquals(99, page.getInt(32));
    }

    @Test
    void storesByteArraysWithLengthPrefixAtDifferentOffsets() {
        Page page = new Page(256);
        byte[] payload = "payload".getBytes(StandardCharsets.US_ASCII);
        byte[] other = "other".getBytes(StandardCharsets.US_ASCII);

        page.setBytes(4, payload);
        page.setBytes(128, other);

        assertArrayEquals(payload, page.getBytes(4));
        assertArrayEquals(other, page.getBytes(128));
    }

    @Test
    void storesStringsUsingConfiguredCharset() {
        Page page = new Page(256);
        page.setString(0, "hello");
        page.setString(64, "world");

        assertEquals("hello", page.getString(0));
        assertEquals("world", page.getString(64));
    }

    @Test
    void contentsAlwaysResetsPosition() {
        Page page = new Page(32);

        ByteBuffer first = page.contents();
        first.position(10);

        ByteBuffer second = page.contents();
        assertEquals(0, second.position());
    }

    @Test
    void respectsMaxLengthComputationAndBoundaries() {
        int strLen = 10;
        int required = Page.maxLength(strLen);
        Page page = new Page(required + Integer.BYTES);

        String payload = "x".repeat(strLen);
        page.setString(0, payload);

        assertEquals(payload, page.getString(0));
    }
}
