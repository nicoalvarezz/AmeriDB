package io.github.nicoalvarezz.buffer;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private final ByteBuffer byteBuffer;
    public static final Charset CHARSET = StandardCharsets.US_ASCII;

    /**
     * A constructor fo creating data buffers
     */
    public Page(int blockSize) {
      byteBuffer = ByteBuffer.allocateDirect(blockSize);
    }

    /**
     * A constructor for creating log pages
     */
    public Page(byte[] bytes) {
        byteBuffer = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset) {
        return byteBuffer.getInt(offset);
    }

    public void setInt(int offset, int n) {
        byteBuffer.putInt(offset, n);
    }

    public long getLong(int offset) {
        return byteBuffer.getLong(offset);
    }

    public void setLong(int offset, long n) {
        byteBuffer.putLong(offset, n);
    }

    public byte[] getBytes(int offset) {
        byteBuffer.position(offset);
        int length  = byteBuffer.getInt();
        byte[] b = new byte[length];
        byteBuffer.get(b);
        return b;
    }

    public void setBytes(int offset, byte[] b) {
        byteBuffer.position(offset);
        byteBuffer.putInt(b.length);
        byteBuffer.put(b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES  + (strlen * (int) bytesPerChar);
    }

    public ByteBuffer contents() {
        byteBuffer.position(0);
        return byteBuffer;
    }
}
