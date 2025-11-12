package io.github.nicoalvarezz.file;

import java.util.Objects;

public class Block {
    private final String filename;
    private final int blockNumber;

    public Block(String filename, int blockNumber) {
        this.filename = filename;
        this.blockNumber = blockNumber;
    }

    public String fileName() {
        return filename;
    }

    public int number() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Block block = (Block) o;
        return blockNumber == block.blockNumber && Objects.equals(filename, block.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, blockNumber);
    }

    @Override
    public String toString() {
        return "Block{" +
                "filename='" + filename + '\'' +
                ", blockNumber=" + blockNumber +
                '}';
    }
}
