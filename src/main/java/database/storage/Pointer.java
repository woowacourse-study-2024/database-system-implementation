package database.storage;

import java.nio.ByteBuffer;

public class Pointer {

    public static final int SIZE = 4 + 4;

    private final int pageNumber;
    private final int offset;

    public Pointer(int pageNumber, int offset) {
        this.pageNumber = pageNumber;
        this.offset = offset;
    }

    public static Pointer deserialize(ByteBuffer buffer) {
        int pageNumber = buffer.getInt();
        int offset = buffer.getInt();
        return new Pointer(pageNumber, offset);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(pageNumber);
        buffer.putInt(offset);
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getOffset() {
        return offset;
    }
}
