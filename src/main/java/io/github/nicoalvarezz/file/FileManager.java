package io.github.nicoalvarezz.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class FileManager {
    private final File dbDirectory;
    private final int blockSize;
    private final boolean isNew;
    private final Map<String, RandomAccessFile> openedFiles = new HashMap<>();

    public FileManager(File dbDirectory, int blockSize) {
        this.dbDirectory = dbDirectory;
        this.blockSize = blockSize;
        isNew = !dbDirectory.exists();

        if (isNew) {
            dbDirectory.mkdirs();
        }
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp")) {
                new File(dbDirectory, filename).delete();
            }
        }
    }

    public synchronized void read(Block block, Page page) {
        try {
            RandomAccessFile file = getFile(block.fileName());
            file.seek((long) block.number() * blockSize);
            file.getChannel().read(page.contents());
        } catch (IOException ex) {
            throw new RuntimeException("cannot read block " + block);
        }
    }

    public synchronized void write(Block block, Page page) {
        try {
            RandomAccessFile file = getFile(block.fileName());
            file.seek((long) block.number() * blockSize);
            file.getChannel().write(page.contents());
        } catch (IOException ex) {
            throw new RuntimeException("cannot write block " + block);
        }
    }

    public synchronized Block append(String filename) {
        int newBlockNumber = length(filename);
        Block block = new Block(filename, newBlockNumber);
        byte[] bytes = new byte[blockSize];
        try {
            RandomAccessFile file = getFile(block.fileName());
            file.seek((long) block.number() * blockSize);
            file.write(bytes);
        } catch (IOException ex) {
            throw new RuntimeException("cannot access " +  filename);
        }
        return block;
    }

    public int length(String filename) {
        try {
            RandomAccessFile file = getFile(filename);
            return (int) (file.length() / blockSize);
        } catch (IOException ex) {
            throw new RuntimeException("cannot access " + filename);
        }
    }

    public boolean isNew() {
        return isNew;
    }

    public int blockSize() {
        return blockSize;
    }

    private RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile file = openedFiles.get(filename);
        if (file == null) {
            File dbTable = new File(dbDirectory, filename);
            file = new RandomAccessFile(dbTable, "rws");
            openedFiles.put(filename, file);
        }
        return file;
    }
}
