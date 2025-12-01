package io.github.nicoalvarezz.storage;

import java.nio.ByteBuffer;

public class Page {
    private final int id;
    private int pinCount;
    private boolean isDirty;
//    private final ByteBuffer data;

    public Page(int id) {
        this.id = 0;
    }
}
