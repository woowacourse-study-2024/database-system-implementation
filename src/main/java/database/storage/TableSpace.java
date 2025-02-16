package database.storage;

import database.storage.bufferpool.BufferPool;
import database.storage.bufferpool.PageId;
import database.storage.page.Data;
import database.storage.page.FspHeader;
import database.storage.page.Page;
import database.storage.page.Record;

public class TableSpace {

    private final String fileName;
    private final FspHeader fspHeader;

    private final BufferPool bufferPool;

    public TableSpace(String fileName, BufferPool bufferPool) {
        this.fileName = fileName;
        this.fspHeader = (FspHeader) bufferPool.getPage(new PageId(fileName, 0));
        this.bufferPool = bufferPool;
    }

    public Page allocateFreePage() {
        int pageNumber = fspHeader.allocatePage();
        Page page = Data.createNew(pageNumber);
        return bufferPool.putPage(new PageId(fileName, pageNumber), page);
    }

    public void deallocatePage(Page page) {
        int pageNumber = page.getPageNumber();
        fspHeader.deallocatePage(pageNumber);
        bufferPool.putPage(new PageId(fileName, pageNumber), page);
    }

    public void insertRecord(int rootPageNumber, Record record) {

    }
}
