package database.storage.page;

import java.nio.ByteBuffer;

public class PageHeader {

    public static final int SIZE = 2 + 2 + 2 + 1;

    private static final int TRUE = 1;
    private static final int FALSE = 0;

    private short pageLevel;
    private short lastInsertOffset;
    private short recordCount;
    private boolean isDirty;

    public PageHeader(short pageLevel, short lastInsertOffset, short recordCount, boolean isDirty) {
        this.pageLevel = pageLevel;
        this.lastInsertOffset = lastInsertOffset;
        this.recordCount = recordCount;
        this.isDirty = isDirty;
    }

    public static PageHeader deserialize(ByteBuffer buffer) {
        short pageLevel = buffer.getShort();
        short lastInsertOffset = buffer.getShort();
        short recordCount = buffer.getShort();
        boolean isDirty = buffer.get() == TRUE;

        return new PageHeader(pageLevel, lastInsertOffset, recordCount, isDirty);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putShort(pageLevel);
        buffer.putShort(lastInsertOffset);
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

    public short getLastInsertOffset() {
        return lastInsertOffset;
    }

    public short getRecordCount() {
        return recordCount;
    }

    public boolean isDirty() {
        return isDirty;
    }
}
