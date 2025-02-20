package database.storage.page.fspheader;

import java.nio.ByteBuffer;
import java.util.Objects;

public class Pointer {

    public static final int SIZE = 4 + 4;

    private final int pageNumber;
    private final int offset;

    public Pointer(int pageNumber, int offset) {
        this.pageNumber = pageNumber;
        this.offset = offset;
    }

    public static Pointer createNew() {
        return new Pointer(-1, -1);
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

    public boolean isEmpty() {
        return pageNumber == -1 && offset == -1;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pointer pointer = (Pointer) o;
        return pageNumber == pointer.pageNumber && offset == pointer.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageNumber, offset);
    }
}
