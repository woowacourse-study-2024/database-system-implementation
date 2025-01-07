package database.engine.page;

import java.nio.ByteBuffer;

public class FileHeader {

    private final int pageNumber;
    private final PageType pageType;
    private int prevPageNumber;
    private int nextPageNumber;

    private FileHeader(int pageNumber, PageType pageType, int prevPageNumber, int nextPageNumber) {
        this.pageNumber = pageNumber;
        this.pageType = pageType;
        this.prevPageNumber = prevPageNumber;
        this.nextPageNumber = nextPageNumber;
    }

    public static FileHeader deserialize(ByteBuffer buffer) {
        int pageNumber = buffer.getInt();
        PageType pageType = PageType.fromCode(buffer.get());
        int prevPageNumber = buffer.getInt();
        int nextPageNumber = buffer.getInt();

        return new FileHeader(pageNumber, pageType, prevPageNumber, nextPageNumber);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(pageNumber);
        buffer.put((byte) pageType.getCode());
        buffer.putInt(prevPageNumber);
        buffer.putInt(nextPageNumber);
    }

    public int getPageNumber() {
        return pageNumber;
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
