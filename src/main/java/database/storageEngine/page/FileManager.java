package database.storageEngine.page;

import database.storageEngine.bufferpool.TablePageKey;
import database.storageEngine.exception.DirectoryCreationException;
import database.storageEngine.exception.PageLoadException;
import database.storageEngine.exception.PageSaveException;
import database.storageEngine.exception.TableCreationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileManager {

    private static final int PAGE_SIZE = 16 * 1024;
    private static final String DIRECTORY_PATH = "disk";
    private final FileExtension fileExtension;
    private int pageSize;

    public FileManager(FileExtension fileExtension) {
        createDirectoryIfNotExists();
        this.pageSize = 0;
        this.fileExtension = fileExtension;
    }

    private void createDirectoryIfNotExists() {
        Path path = Paths.get(DIRECTORY_PATH);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw new DirectoryCreationException("Failed to create directory: " + DIRECTORY_PATH, e);
            }
        }
    }

    public void savePage(String tableName, Page page) {
        createTableIfNotExists(tableName);
        String fileName = DIRECTORY_PATH + File.separator + tableName + fileExtension.getExtension();

        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
            file.seek(page.getPageNumber() * PAGE_SIZE);

            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file.getFD()))) {
                out.writeObject(page);
            }
            pageSize++;
        } catch (IOException e) {
            throw new PageSaveException("Failed to save page for table: " + tableName, e);
        }
    }

    private void createTableIfNotExists(String tableName) {
        String fileName = DIRECTORY_PATH + File.separator + tableName + fileExtension.getExtension();
        File file = new File(fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new TableCreationException("Failed to create table file: " + tableName, e);
            }
        }
    }

    public Page loadPage(TablePageKey key) {
        createTableIfNotExists(key.tableName());
        String fileName = DIRECTORY_PATH + File.separator + key.tableName() + fileExtension.getExtension();

        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            file.seek(key.pageNumber() * PAGE_SIZE);

            try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file.getFD()))) {
                return (Page) in.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new PageLoadException("Failed to load page for key: " + key, e);
        }
    }

    public Page createNewDataPage() {
        long newPageNumber = getNewPageNumber();
        return PageFactory.createDataPage(newPageNumber);
    }

    private int getNewPageNumber() {
        return pageSize;
    }
}
