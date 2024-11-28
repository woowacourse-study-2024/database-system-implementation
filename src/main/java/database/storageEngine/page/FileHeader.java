package database.storageEngine.page;

import java.io.Serializable;

public class FileHeader implements Serializable {

    private static final int HEADER_SIZE = 38;

    private final long pageNumber;

    public FileHeader(long pageNumber) {
        this.pageNumber = pageNumber;
    }

    public long getPageNumber() {
        return pageNumber;
    }

    public int getHeaderSize() {
        return HEADER_SIZE;
    }

    @Override
    public String toString() {
        return "FileHeader{" +
                "pageNumber=" + pageNumber +
                '}';
    }
}
