package database.page;

import java.util.LinkedList;

public class LRUReplacementPolicy implements PageReplacementPolicy {

    private final LinkedList<PageId> lruList;

    public LRUReplacementPolicy(LinkedList<PageId> lruList) {
        this.lruList = lruList;
    }

    @Override
    public void addPage(PageId pageId) {
        lruList.addFirst(pageId);
    }

    @Override
    public void updatePage(PageId pageId) {
        lruList.remove(pageId);
        lruList.addFirst(pageId);
    }

    @Override
    public PageId evictPage() {
        return lruList.removeLast();
    }
}
