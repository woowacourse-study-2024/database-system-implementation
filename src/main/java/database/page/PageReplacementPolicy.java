package database.page;

public interface PageReplacementPolicy {

    void addPage(PageId pageId);

    void updatePage(PageId pageId);

    PageId evictPage();
}
