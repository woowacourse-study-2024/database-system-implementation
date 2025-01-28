package database.page;

public class Page {
    private final int pageNum;
    private byte[] data;
    private boolean dirty;

    public Page(int pageNum, byte[] data) {
        this.pageNum = pageNum;
        this.data = data;
        dirty = false;
    }

    public int getPageNum() {
        return pageNum;
    }

    public byte[] getData() {
        return data;
    }

    public void updateData(byte[] data) {
        this.data = data;
        dirty = true;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setClean() {
        dirty = false;
    }
}
