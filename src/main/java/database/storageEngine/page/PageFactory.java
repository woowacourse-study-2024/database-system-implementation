package database.storageEngine.page;

public class PageFactory {

    public static Page createDataPage(long pageNumber) {
        return new Page(pageNumber, PageType.PAGE_TYPE_CLUSTERED);
    }

    public static Page createIndexPage(long pageNumber) {
        return new Page(pageNumber, PageType.PAGE_TYPE_BTR_INDEX);
    }

    public static Page createUndoPage(long pageNumber) {
        return new Page(pageNumber, PageType.PAGE_TYPE_UNDO);
    }
}
