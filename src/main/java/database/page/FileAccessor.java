package database.page;

import java.io.IOException;
import java.io.RandomAccessFile;

public class FileAccessor {
    private final RandomAccessFile file;
    private final int pageSize;
    private final int headerSize;

    public FileAccessor(RandomAccessFile file, int pageSize, int headerSize) {
        this.file = file;
        this.pageSize = pageSize;
        this.headerSize = headerSize;
    }

    public void writePage(int pageNum, byte[] data) throws IOException {
        long offset = calculatePageOffset(pageNum);
        file.seek(offset);
        file.write(data);
    }

    public void readPage(int pageNum, byte[] buffer) throws IOException {
        long offset = calculatePageOffset(pageNum);
        file.seek(offset);
        file.readFully(buffer);
    }

    public void clearPage(int pageNum) throws IOException {
        long offset = calculatePageOffset(pageNum);
        file.seek(offset);
        file.write(new byte[pageSize]);
    }

    private long calculatePageOffset(int pageNum) {
        return headerSize + (long) pageNum * pageSize;
    }

    public void writeHeader(byte[] headerData) throws IOException {
        file.seek(0);
        file.write(headerData);
    }

    public void readHeader(byte[] buffer) throws IOException {
        file.seek(0);
        file.readFully(buffer);
    }

    public long getFileLength() throws IOException {
        return file.length();
    }

    public boolean isFileEmpty() throws IOException {
        return file.length() == 0;
    }

    public void close() throws IOException {
        file.close();
    }
}
