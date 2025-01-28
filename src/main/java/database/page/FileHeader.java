package database.page;

import java.io.IOException;
import java.util.PriorityQueue;

public class FileHeader {
    static final int HEADER_SIZE = 16 * 1024;

    private final byte[] pageAllocationBitMap;
    private final PriorityQueue<Integer> freePages;
    private final FileAccessor fileAccessor;
    private int numPages;

    public FileHeader(
            FileAccessor fileAccessor,
            byte[] pageAllocationBitMap,
            PriorityQueue<Integer> freePages
    ) throws IOException {
        this.fileAccessor = fileAccessor;
        this.pageAllocationBitMap = pageAllocationBitMap;
        this.freePages = freePages;

        if (fileAccessor.isFileEmpty()) {
            fileAccessor.writeHeader(pageAllocationBitMap);
        } else {
            fileAccessor.readHeader(pageAllocationBitMap);
        }

        this.numPages = (int) (fileAccessor.getFileLength() - HEADER_SIZE) / PagedFile.PAGE_SIZE;
        for (int i = 0; i < numPages; i++) {
            if (!isPageAllocated(i)) {
                freePages.add(i);
            }
        }
    }

    public boolean isPageAllocated(int pageNum) {
        int byteIndex = pageNum / 8;
        int bitIndex = pageNum % 8;
        return (pageAllocationBitMap[byteIndex] & (1 << bitIndex)) != 0;
    }

    public int allocatePage() throws IOException {
        int pageNum;
        if (!freePages.isEmpty()) {
            pageNum = freePages.poll();
        } else {
            pageNum = numPages++;
        }

        int byteIndex = pageNum / 8;
        int bitIndex = pageNum % 8;
        pageAllocationBitMap[byteIndex] |= (byte) (1 << bitIndex);

        fileAccessor.writeHeader(pageAllocationBitMap);

        return pageNum;
    }

    public void deallocatePage(int pageNum) throws IOException {
        int byteIndex = pageNum / 8;
        int bitIndex = pageNum % 8;
        pageAllocationBitMap[byteIndex] &= (byte) ~(1 << bitIndex);
        freePages.add(pageNum);

        fileAccessor.writeHeader(pageAllocationBitMap);
    }
}
