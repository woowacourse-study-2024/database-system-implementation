package database;

import database.engine.page.FileExtension;
import database.engine.page.Page;
import java.util.HashMap;
import java.util.Map;

public class BufferPool {

    private final Map<PageId, Page> pages;
    private final PageReplacementStrategy<PageId> strategy;
    private final FileManager fileManager;
    private final int capacity;

    public BufferPool(PageReplacementStrategy<PageId> strategy, int capacity) {
        this.pages = new HashMap<>();
        this.strategy = strategy;
        this.fileManager = new FileManager(FileExtension.IDB);
        this.capacity = capacity;
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

        if (pages.size() >= capacity) {
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
}
