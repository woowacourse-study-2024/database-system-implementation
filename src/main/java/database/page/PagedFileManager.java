package database.page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class PagedFileManager {
    private final Map<String, PagedFile> openFiles;

    public PagedFileManager(Map<String, PagedFile> openFiles) {
        this.openFiles = openFiles;
    }

    public PagedFile createFile(String filename) throws IOException {
        File file = new File(filename);
        if (file.exists()) {
            throw new IOException("File already exists.");
        }

        if (!file.createNewFile()) {
            throw new IOException("Failed to create file.");
        }

        return new PagedFile(new RandomAccessFile(file, "rw"));
    }

    public void deleteFile(String filename) throws IOException {
        File file = new File(filename);
        if (!file.exists()) {
            throw new IOException("File not found.");
        }

        if (openFiles.containsKey(filename)) {
            throw new IOException("File is currently open and cannot be deleted.");
        }

        if (!file.delete()) {
            throw new IOException("Failed to delete file.");
        }
    }

    public PagedFile openFile(String filename) throws IOException {
        if (openFiles.containsKey(filename)) {
            return openFiles.get(filename);
        }

        File file = new File(filename);
        if (!file.exists()) {
            throw new IOException("File not found.");
        }

        PagedFile pagedFile = new PagedFile(new RandomAccessFile(file, "rw"));
        openFiles.put(filename, pagedFile);

        return pagedFile;
    }

    public void closeFile(String filename) throws IOException {
        PagedFile pagedFile = openFiles.get(filename);
        if (pagedFile == null) {
            throw new IOException("File is not open.");
        }

        pagedFile.close();
        openFiles.remove(filename);
    }
}
