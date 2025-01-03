package database.engine.page;

public class PageHeader {

    private short pageLevel;
    private short lastInsertPosition;
    private short recordCount;
    private boolean isDirty;

    public PageHeader(short pageLevel) {
        this.pageLevel = pageLevel;
        this.lastInsertPosition = -1;
        this.recordCount = 0;
        this.isDirty = false;
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
