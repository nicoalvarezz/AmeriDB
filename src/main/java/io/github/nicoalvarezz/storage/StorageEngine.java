package io.github.nicoalvarezz.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static io.github.nicoalvarezz.storage.StorageConfig.BLOCK_SIZE;

public class StorageEngine {
    private final File storageDir;
    private final Map<String, RandomAccessFile> openedFiles = new HashMap<>();

    public StorageEngine(String dbName) {
        String homeDir = System.getProperty("user.home");
        File requested = new File(dbName);
        storageDir = requested.isAbsolute() ? requested : new File(homeDir, dbName);
        boolean isNew = !storageDir.exists();
        if (isNew && !storageDir.mkdirs()) {
            throw new RuntimeException("Database cannot be created.");
        }

        // remove any leftover temporary tables
        String[] existing = storageDir.list();
        if (existing != null) {
            for (String filename : existing) {
                if (filename.startsWith("temp")) {
                    new File(storageDir, filename).delete();
                }
            }
        }
    }

    public synchronized void read(BlockId blockId, ByteBuffer byteBuffer) {
       try {
           RandomAccessFile file = file(blockId.filename());
           file.seek((long) blockId.number() * BLOCK_SIZE);
           file.getChannel().read(byteBuffer);
       } catch (IOException ex) {
           throw new RuntimeException("Cannot read block");
       }
    }

    public synchronized void write(BlockId blockId, ByteBuffer bufferBuffer) {
        try {
            RandomAccessFile file = file(blockId.filename());
            file.seek((long) blockId.number() * BLOCK_SIZE);
            file.getChannel().write(bufferBuffer);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot write block", ex);
        }
    }

    public synchronized BlockId append(String filename) {
        // TODO(nico): We are accessing the file twice here; 1. length of file, 2. file to append new block
        BlockId blockId = new BlockId(filename, length(filename));
        byte[] bytes = new byte[BLOCK_SIZE];
        try {
            RandomAccessFile file = file(filename);
            file.seek((long) blockId.number() * BLOCK_SIZE);
            file.write(bytes);
        } catch (IOException ex) {
            throw new RuntimeException("Cannot access", ex);
        }
        return blockId;
    }

    public int length(String filename) {
        try {
            RandomAccessFile file = file(filename);
            return (int) ((file.length() / BLOCK_SIZE));
        } catch (IOException ex) {
            throw new RuntimeException("Cannot access", ex);
        }
    }

    public RandomAccessFile file(String filename) throws IOException {
        RandomAccessFile file = openedFiles.get(filename);
        if (file == null) {
            File dbTable = new File(storageDir, filename);
            file = new RandomAccessFile(dbTable, "rws");
            openedFiles.put(filename, file);
        }
        return file;
    }
}
