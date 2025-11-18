package io.github.nicoalvarezz.wal;

public record Lsn(long value) implements Comparable<Lsn> {

    /**
     * Compares log sequence numbers.
     * @param other the object to be compared.
     * @return the value 0 if x == y; a value less than 0 if x < y; and a value greater than 0 if x > y
     *
     */
    @Override
    public int compareTo(Lsn other) {
        return Long.compare(this.value, other.value);
    }
}
