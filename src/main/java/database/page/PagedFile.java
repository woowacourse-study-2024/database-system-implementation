package database.page;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.PriorityQueue;

public class PagedFile {
    public static final int PAGE_SIZE = 16 * 1024;

    private final FileAccessor fileAccessor;
    private final FileHeader fileHeader;

    public PagedFile(RandomAccessFile file) throws IOException {
        this.fileAccessor = new FileAccessor(file, PAGE_SIZE, FileHeader.HEADER_SIZE);
        this.fileHeader = new FileHeader(fileAccessor, new byte[FileHeader.HEADER_SIZE], new PriorityQueue<>());
    }

    public int allocatePage() throws IOException {
        int pageNum = fileHeader.allocatePage();
        fileAccessor.clearPage(pageNum);
        return pageNum;
    }

    public void deallocatePage(int pageNum) throws IOException {
        fileHeader.deallocatePage(pageNum);
        fileAccessor.clearPage(pageNum);
    }

    public void writePage(int pageNum, byte[] data) throws IOException {
        fileAccessor.writePage(pageNum, data);
    }

    public Page readPage(int pageNum) throws IOException {
        byte[] data = new byte[PAGE_SIZE];
        fileAccessor.readPage(pageNum, data);
        return new Page(pageNum, data);
    }

    public void close() throws IOException {
        fileAccessor.close();
    }
}
