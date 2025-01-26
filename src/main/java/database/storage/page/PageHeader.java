package database.storage.page;

import java.nio.ByteBuffer;

public class PageHeader {

    public static final int HEADER_SIZE = 2 + 2 + 2 + 1;

    private static final int TRUE = 1;
    private static final int FALSE = 0;

    private short pageLevel;
    private short lastInsertPosition;
    private short recordCount;
    private boolean isDirty;

    private PageHeader(short pageLevel, short lastInsertPosition, short recordCount, boolean isDirty) {
        this.pageLevel = pageLevel;
        this.lastInsertPosition = lastInsertPosition;
        this.recordCount = recordCount;
        this.isDirty = isDirty;
    }

    public static PageHeader create(short pageLevel, short lastInsertPosition, short recordCount, boolean isDirty) {
        return new PageHeader(pageLevel, lastInsertPosition, recordCount, isDirty);
    }

    public static PageHeader deserialize(ByteBuffer buffer) {
        short pageLevel = buffer.getShort();
        short lastInsertPosition = buffer.getShort();
        short recordCount = buffer.getShort();
        boolean isDirty = buffer.get() != FALSE;

        return new PageHeader(pageLevel, lastInsertPosition, recordCount, isDirty);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putShort(pageLevel);
        buffer.putShort(lastInsertPosition);
        buffer.putShort(recordCount);
        buffer.put((byte) (isDirty ? TRUE : FALSE));
    }

    public void makeClean() {
        isDirty = false;
    }

    public void makeDirty() {
        isDirty = true;
    }

    public short getPageLevel() {
        return pageLevel;
    }

    public short getLastInsertPosition() {
        return lastInsertPosition;
    }

    public short getRecordCount() {
        return recordCount;
    }

    public boolean isDirty() {
        return isDirty;
    }
}
