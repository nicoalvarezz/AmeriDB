package io.github.nicoalvarezz.wal;

public abstract class LogRecord {
    public abstract Lsn lsn();
    public abstract byte[] payload();
}
