package io.github.nicoalvarezz.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.nicoalvarezz.common.Config.PAGE_SIZE;

/**
 * Manages all physical I/O operations for a multi-file database.
 * Each logical database object (like table or an index) is mapped to its own
 * file.
 * The DiskManger handles transition of (fileName, pageId) into a file access
 * operation.
 */
public class DiskManager {

    // The base directory where all database files reside
    private final String dbPath;

    // Latch to serialize operations that access or modify the file state
    private final ReentrantLock fileMutex;

    // Cache: Maps logical file names (e.g., "table_users.data") to their active
    // FileChannel object
    private final ConcurrentHashMap<String, FileChannel> fileChannelCache;

    // Manages the next available pageId per file. Key is the file name, value is
    // the next pageId
    private final ConcurrentHashMap<String, AtomicInteger> nextPageIdMap;

    /**
     * Constructor: Initializes the DiskManager, setting up the base directory.
     * 
     * @param dbPath The path to the database directory (e.g., "/var/lib/mydb/").
     * @throws IOException if the directory cannot be created or accessed.
     */
    public DiskManager(String dbPath) throws IOException {
        this.dbPath = dbPath;
        File dbDir = new File(dbPath);

        // Ensure the database directory exists
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new IOException("Failed to create database directory: " + dbPath);
            }
        }
        this.fileMutex = new ReentrantLock();
        this.fileChannelCache = new ConcurrentHashMap<>();
        this.nextPageIdMap = new ConcurrentHashMap<>();

        initializeFileState(dbDir);
    }

    /**
     * Reads a page from the specified file into the provided buffer
     * 
     * @param fileName The logical file name (e.g., "users_table.data")
     * @param pageId   The logical ID of the page within that file
     * @param data     The pre-allocated ByteBuffer to load the data into
     */
    public void readPage(String fileName, int pageId, ByteBuffer data) throws IOException {
        FileChannel channel = getFileChannel(fileName);
        long offset = getFileOffset(pageId);

        data.clear();
        channel.read(data, offset);

        // Standard safety check: If less than a full page was read, pad with zeros
        if (data.position() < PAGE_SIZE) {
            while (data.position() < PAGE_SIZE) {
                data.put((byte) 0);
            }
        }
        data.flip();
    }

    /**
     * Writes the contents opf memory buffer to the specified file.
     * 
     * @param fileName The logical file name (e.g., "users_table.data").
     * @param pageId   The logical ID of the page within that file.
     * @param data     The BytBuffer containing the page data to be written
     */
    public void writePage(String fileName, int pageId, ByteBuffer data) throws IOException {
        FileChannel channel = getFileChannel(fileName);
        long offset = getFileOffset(pageId);

        data.rewind();
        channel.write(data, offset);
        channel.force(true); // Essential for durability (ACID) ensure data is flushed to physical disk
    }

    /**
     * Allocates a new page and reserves its ID within the context of a specific
     * file.
     * 
     * @param fileName The file to allocate the new page in.
     * @return The ID of the newly allocated page.
     */
    public int allocatePage(String fileName) throws IOException {
        getFileChannel(fileName);

        AtomicInteger counter = nextPageIdMap.get(fileName);
        if (counter == null) {
            // Should not happen if getFileChannel was called, but a safeguard.
            throw new IOException("File not tracked for allocation " + fileName);
        }
        return counter.getAndIncrement();
    }

    /**
     * Closes all cached file channels and cleans up resources.
     */
    public void shutDown() throws IOException {
        fileMutex.lock();
        try {
            for (FileChannel channel : fileChannelCache.values()) {
                channel.close();
            }
            fileChannelCache.clear();
            nextPageIdMap.clear();
        } finally {
            fileMutex.unlock();
        }
    }

    private void initializeFileState(File dbDir) throws IOException {
        File[] files = dbDir.listFiles((dir, name) -> name.endsWith(".data"));
        if (files != null) {
            for (File file : files) {
                String fileName = file.getName();

                // Open channel and cache
                FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
                fileChannelCache.put(fileName, channel);

                // Calculate the next page ID based on the file size
                long fileSize = channel.size();
                int lastPageId = (int) (fileSize / PAGE_SIZE);
                nextPageIdMap.put(fileName, new AtomicInteger(lastPageId));
            }
        }
    }

    private FileChannel getFileChannel(String fileName) throws IOException {
        FileChannel channel = fileChannelCache.get(fileName);
        if (channel == null) {
            // Lazily create and cache the channel if it's accessed for the first time.
            fileMutex.lock(); // Latch acquired to protect file creation
            try {
                channel = fileChannelCache.get(fileName);
                if (channel == null) {
                    File fileHandle = new File(dbPath, fileName);
                    channel = FileChannel.open(fileHandle.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
                    fileChannelCache.put(fileName, channel);

                    // Initialise the nextPageId counter for the new file
                    nextPageIdMap.put(fileName, new AtomicInteger(0));
                }
            } finally {
                fileMutex.unlock();
            }
        }
        return channel;
    }

    private long getFileOffset(int pageId) {
        return (long) pageId * PAGE_SIZE;
    }
}
