package database.storage.page;

import java.nio.ByteBuffer;

public abstract class AbstractPage implements Page {

    protected FileHeader fileHeader;
    // FileTrailer fileTrailer;

    @Override
    public void serialize(ByteBuffer buffer) {
        fileHeader.serialize(buffer);
        serializeBody(buffer);
    }

    @Override
    public int getPageNumber() {
        return fileHeader.getPageNumber();
    }

    protected abstract void serializeBody(ByteBuffer buffer);

    public FileHeader getFileHeader() {
        return fileHeader;
    }
}
