package database.storage.bufferpool;

import database.storage.bufferpool.io.FileManager;
import database.storage.bufferpool.strategy.PageReplacementStrategy;
import database.storage.page.Data;
import database.storage.page.Page;
import java.util.HashMap;
import java.util.Map;

public class BufferPool {

    private final Map<PageId, Page> pages;
    private final PageReplacementStrategy<PageId> strategy;

    private final FileManager fileManager;

    public BufferPool(PageReplacementStrategy<PageId> strategy, FileManager fileManager) {
        this.pages = new HashMap<>();
        this.strategy = strategy;
        this.fileManager = fileManager;
    }

    public Page getPage(PageId pageId) {
        if (pages.containsKey(pageId)) {
            strategy.access(pageId);
            return pages.get(pageId);
        }

        Page page = fileManager.loadPage(pageId.fileName(), pageId.pageNumber());
        strategy.put(pageId);
        pages.put(pageId, page);
        return page;
    }

    public Page putPage(PageId pageId, Page page) {
        if (pages.containsKey(pageId)) {
            strategy.access(pageId);
            return pages.get(pageId);
        }

        if (strategy.shouldEvict()) {
            evictPage();
        }

        strategy.put(pageId);
        pages.put(pageId, page);
        return page;
    }

    public void flushPage(PageId pageId) {
        if (!pages.containsKey(pageId)) {
            return;
        }

        Data page = (Data) pages.get(pageId);
        if (!page.isDirty()) {
            return;
        }

        fileManager.writePage(page, pageId.fileName());
        page.makeClean();
    }

    public boolean containsPage(PageId pageId) {
        return pages.containsKey(pageId);
    }

    private void evictPage() {
        PageId evicted = strategy.evict();
        if (evicted != null) {
            pages.remove(evicted);
        }
    }
}
