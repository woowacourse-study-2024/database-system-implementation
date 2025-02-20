package database.storage.io;

import database.storage.page.Page;
import database.storage.page.PageFactory;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileManager {

    public static final String DIRECTORY_PATH = "files/data/";

    private static final int MAX_TIME_TO_BLOCK_MS = 2000;

    private final ByteBufferPool byteBufferPool;
    private final FileExtension fileExtension;

    public FileManager(ByteBufferPool byteBufferPool, FileExtension fileExtension) {
        this.byteBufferPool = byteBufferPool;
        this.fileExtension = fileExtension;
        createDirectory();
    }

    private void createDirectory() {
        Path directory = Paths.get(DIRECTORY_PATH);

        if (Files.exists(directory)) {
            return;
        }

        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Page loadPage(String fileName, int pageNumber) {
        Path filePath = Paths.get(DIRECTORY_PATH, fileName + fileExtension.getExtension());
        ByteBuffer buffer = null;

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            long offset = (long) pageNumber * Page.SIZE;
            buffer = byteBufferPool.allocate(MAX_TIME_TO_BLOCK_MS);
            channel.read(buffer, offset);

            buffer.flip();

            return PageFactory.deserialize(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page " + pageNumber + " for table " + fileName + ".", e);
        } finally {
            byteBufferPool.deallocate(buffer);
        }
    }

    public void writePage(Page page, String fileName) {
        Path filePath = Paths.get(DIRECTORY_PATH, fileName + fileExtension.getExtension());
        int pageNumber = page.getPageNumber();
        ByteBuffer buffer = null;

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            long offset = (long) pageNumber * Page.SIZE;
            buffer = byteBufferPool.allocate(MAX_TIME_TO_BLOCK_MS);
            page.serialize(buffer);

            buffer.rewind();

            channel.write(buffer, offset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write page " + pageNumber + " for table " + fileName + ".", e);
        } finally {
            byteBufferPool.deallocate(buffer);
        }
    }
}
