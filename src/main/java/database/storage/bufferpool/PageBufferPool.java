package database.storage.bufferpool;

import database.storage.bufferpool.io.ByteBufferPool;
import database.storage.bufferpool.io.FileManager;
import database.storage.bufferpool.strategy.PageReplacementStrategy;
import database.storage.page.FileExtension;
import database.storage.page.Page;
import java.util.HashMap;
import java.util.Map;

public class PageBufferPool {

    private final Map<PageId, Page> pages;
    private final PageReplacementStrategy<PageId> strategy;
    private final FileManager fileManager;

    public PageBufferPool(PageReplacementStrategy<PageId> strategy, long totalMemory) {
        this.pages = new HashMap<>();
        this.strategy = strategy;
        this.fileManager = new FileManager(getByteBufferPool(totalMemory), FileExtension.IDB);
    }

    public Page getPage(PageId pageId) {
        if (pages.containsKey(pageId)) {
            strategy.access(pageId);
            return pages.get(pageId);
        }

        Page page = fileManager.loadPage(pageId.fileName(), pageId.pageNumber());
        strategy.put(pageId);
        return page;
    }

    public void putPage(PageId pageId, Page page) {
        if (pages.containsKey(pageId)) {
            strategy.access(pageId);
            return;
        }

        if (strategy.shouldEvict()) {
            evictPage(pageId);
        }

        pages.put(pageId, page);
        strategy.put(pageId);
    }

    public void flushPage(PageId pageId) {
        if (!pages.containsKey(pageId)) {
            return;
        }

        Page page = pages.get(pageId);
        if (!page.isDirty()) {
            return;
        }

        fileManager.writePage(page, pageId.fileName());
        page.makeClean();
    }

    public void flushAllPages() {
        for (PageId pageId : strategy.keySet()) {
            flushPage(pageId);
        }
    }

    public void removePage(PageId pageId) {
        if (!pages.containsKey(pageId)) {
            return;
        }

        Page page = pages.get(pageId);
        if (page.isDirty()) {
            flushPage(pageId);
        }

        strategy.remove(pageId);
        pages.remove(pageId);
    }

    public boolean containsPage(PageId pageId) {
        return pages.containsKey(pageId);
    }

    private void evictPage(PageId pageId) {
        PageId evicted = strategy.evict();
        if (evicted != null && pages.containsKey(evicted)) {
            flushPage(pageId);
            pages.remove(pageId);
        }
    }

    private ByteBufferPool getByteBufferPool(long totalMemory) {
        long defaultMemorySize = totalMemory / 10;
        long minimumMemorySize = Page.PAGE_SIZE * 30;
        return new ByteBufferPool(Math.max(defaultMemorySize, minimumMemorySize), Page.PAGE_SIZE);
    }
}
