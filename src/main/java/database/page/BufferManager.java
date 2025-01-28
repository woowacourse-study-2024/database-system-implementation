package database.page;

import java.io.IOException;

public class BufferManager {
    private final Buffer buffer;
    private final PageReplacementPolicy pageReplacementPolicy;
    private final PagedFileManager pagedFileManager;
    private final ScratchPageManager scratchPageManager;

    public BufferManager(
            Buffer buffer,
            PageReplacementPolicy pageReplacementPolicy,
            PagedFileManager pagedFileManager,
            ScratchPageManager scratchPageManager
    ) {
        this.buffer = buffer;
        this.pageReplacementPolicy = pageReplacementPolicy;
        this.pagedFileManager = pagedFileManager;
        this.scratchPageManager = scratchPageManager;
    }

    public Page getPage(PageId pageId) throws IOException {
        if (scratchPageManager.isScratchPage(pageId)) {
            return buffer.getPage(pageId);
        }
        if (buffer.containsPage(pageId)) {
            pageReplacementPolicy.updatePage(pageId);
            return buffer.getPage(pageId);
        }

        PagedFile pagedFile = pagedFileManager.openFile(pageId.filename());

        Page page = pagedFile.readPage(pageId.pageNum());
        if (buffer.isFull()) {
            evictPage();
        }

        buffer.putPage(pageId, page);
        pageReplacementPolicy.addPage(pageId);
        return page;
    }

    public void flushPage(PageId pageId) throws IOException {
        if (isFlushAble(pageId)) {
            Page page = buffer.getPage(pageId);
            PagedFile pagedFile = pagedFileManager.openFile(pageId.filename());
            pagedFile.writePage(pageId.pageNum(), page.getData());
            page.setClean();
        }
    }

    public void flushAllPages() throws IOException {
        for (PageId pageId : buffer.getPageIds()) {
            if (isFlushAble(pageId)) {
                Page page = buffer.getPage(pageId);
                PagedFile pagedFile = pagedFileManager.openFile(pageId.filename());
                pagedFile.writePage(page.getPageNum(), page.getData());
                page.setClean();
            }
        }
    }

    public Page createScratchPage(byte[] data) throws IOException {
        if (buffer.isFull()) {
            evictPage();
        }

        PageId scratchPageId = scratchPageManager.allocateScratchPageId();
        Page scratchPage = new Page(scratchPageId.pageNum(), data);
        buffer.putPage(scratchPageId, scratchPage);

        return scratchPage;
    }

    public void releaseScratchPage(PageId pageId) {
        if (!scratchPageManager.isScratchPage(pageId)) {
            throw new IllegalArgumentException("Page is not a scratch page");
        }
        buffer.removePage(pageId);
    }

    private void evictPage() throws IOException {
        PageId pageIdToEvict = pageReplacementPolicy.evictPage();
        Page pageToEvict = buffer.removePage(pageIdToEvict);
        String filename = pageIdToEvict.filename();
        if (pageToEvict.isDirty()) {
            PagedFile pagedFile = pagedFileManager.openFile(filename);
            pagedFile.writePage(pageToEvict.getPageNum(), pageToEvict.getData());
        }

        if (!buffer.isReferenced(filename)) {
            pagedFileManager.closeFile(pageIdToEvict.filename());
        }
    }

    private boolean isFlushAble(PageId pageId) {
        if (!buffer.containsPage(pageId)) {
            return false;
        }
        if (scratchPageManager.isScratchPage(pageId)) {
            return false;
        }

        Page page = buffer.getPage(pageId);
        return page.isDirty();
    }
}
