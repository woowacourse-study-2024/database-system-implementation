package database;

import database.engine.page.Page;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileManager {

    private static final String DIRECTORY_PATH = "data/files/";
    private static final String FILE_EXTENSION = ".ibd";

    public FileManager() {
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

    public Page loadPage(String tableName, int pageNumber) {
        Path filePath = Paths.get(DIRECTORY_PATH, tableName + FILE_EXTENSION);

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
             FileChannel channel = raf.getChannel()) {

            long offset = (long) pageNumber * Page.PAGE_SIZE;
            ByteBuffer buffer = ByteBuffer.allocateDirect(Page.PAGE_SIZE);
            channel.read(buffer, offset);

            buffer.flip();

            return Page.deserialize(buffer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load page " + pageNumber + " for table " + tableName + ".", e);
        }
    }
}
