package database;

import database.engine.page.FileExtension;
import database.engine.page.Page;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileManager {

    public static final String DIRECTORY_PATH = "files/data/";

    private final FileExtension fileExtension;

    public FileManager(FileExtension fileExtension) {
        this.fileExtension = fileExtension;
        createDirectory();
    }

    private void createDirectory() {
        Path directory = Paths.get(DIRECTORY_PATH);
        if (Files.notExists(directory)) {
            try {
                Files.createDirectories(directory);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Page loadPage(String fileName, int pageNumber) {
        Path filePath = Paths.get(DIRECTORY_PATH, fileName + fileExtension.getExtension());

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            long offset = (long) pageNumber * Page.PAGE_SIZE;
            ByteBuffer buffer = ByteBuffer.allocateDirect(Page.PAGE_SIZE);
            channel.read(buffer, offset);

            buffer.flip();

            return Page.deserialize(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page " + pageNumber + " for table " + fileName + ".", e);
        }
    }

    public void writePage(Page page, String fileName) {
        Path filePath = Paths.get(DIRECTORY_PATH, fileName + fileExtension.getExtension());
        int pageNumber = page.getPageNumber();

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw");
             FileChannel channel = raf.getChannel()) {

            long offset = (long) pageNumber * Page.PAGE_SIZE;
            ByteBuffer buffer = ByteBuffer.allocateDirect(Page.PAGE_SIZE);
            page.serialize(buffer);

            buffer.rewind();

            channel.write(buffer, offset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write page " + pageNumber + " for table " + fileName + ".", e);
        }
    }
}
