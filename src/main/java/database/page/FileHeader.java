package database.page;

public class FileHeader {

    private final int pageNumber;
    private final int spaceId;
    private final PageType pageType;
    private int prevPageNumber;
    private int nextPageNumber;

    public FileHeader(int pageNumber, int spaceId, PageType pageType, int prevPageNumber, int nextPageNumber) {
        this.pageNumber = pageNumber;
        this.spaceId = spaceId;
        this.pageType = pageType;
        this.prevPageNumber = prevPageNumber;
        this.nextPageNumber = nextPageNumber;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getSpaceId() {
        return spaceId;
    }

    public PageType getPageType() {
        return pageType;
    }

    public int getPrevPageNumber() {
        return prevPageNumber;
    }

    public int getNextPageNumber() {
        return nextPageNumber;
    }
}
