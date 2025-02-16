package database.storage.page;

import java.nio.ByteBuffer;

public class FileHeader {

    public static final int SIZE = 1 + 4 + 4 + 4;

    private final PageType pageType;
    private final int pageNumber;
    private int prevPageNumber;
    private int nextPageNumber;

    public FileHeader(PageType pageType, int pageNumber, int prevPageNumber, int nextPageNumber) {
        this.pageType = pageType;
        this.pageNumber = pageNumber;
        this.prevPageNumber = prevPageNumber;
        this.nextPageNumber = nextPageNumber;
    }

    public static FileHeader createNew(PageType pageType, int pageNumber) {
        return new FileHeader(pageType, pageNumber, -1, -1);
    }

    public static FileHeader deserialize(ByteBuffer buffer) {
        PageType pageType = PageType.fromCode(buffer.get());
        int pageNumber = buffer.getInt();
        int prevPageNumber = buffer.getInt();
        int nextPageNumber = buffer.getInt();

        return new FileHeader(pageType, pageNumber, prevPageNumber, nextPageNumber);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.put((byte) pageType.getCode());
        buffer.putInt(pageNumber);
        buffer.putInt(prevPageNumber);
        buffer.putInt(nextPageNumber);
    }

    public PageType getPageType() {
        return pageType;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getPrevPageNumber() {
        return prevPageNumber;
    }

    public int getNextPageNumber() {
        return nextPageNumber;
    }
}
