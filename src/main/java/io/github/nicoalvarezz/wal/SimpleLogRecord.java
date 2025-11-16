package io.github.nicoalvarezz.wal;

public class SimpleLogRecord extends LogRecord {
    private final Lsn lsn;
    private final byte[] payload;

    public SimpleLogRecord(Lsn lsn, byte[] payload) {
        this.lsn = lsn;
        this.payload = payload;
    }

    @Override
    public Lsn lsn() {
        return lsn;
    }

    @Override
    public byte[] payload() {
        return payload;
    }
}
