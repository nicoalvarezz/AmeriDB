package io.github.nicoalvarezz.wal;

public record Lsn(long value) implements Comparable<Lsn> {
    @Override
    public int compareTo(Lsn other) {
        return Long.compare(this.value, other.value);
    }
}
