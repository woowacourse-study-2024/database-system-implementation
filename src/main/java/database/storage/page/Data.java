package database.storage.page;

import java.nio.ByteBuffer;

public class Data extends AbstractPage {

    private final PageHeader pageHeader;
    private short freeSpace;
    private byte[] recordData;

    public Data(FileHeader fileHeader, PageHeader pageHeader, short freeSpace, byte[] recordData) {
        this.fileHeader = fileHeader;
        this.pageHeader = pageHeader;
        this.recordData = recordData;
        this.freeSpace = freeSpace;
    }

    public static Data deserialize(ByteBuffer buffer) {
        FileHeader fileHeader = FileHeader.deserialize(buffer);
        PageHeader pageHeader = PageHeader.deserialize(buffer);
        short freeSpace = buffer.getShort();
        byte[] recordData = new byte[freeSpace];
        buffer.get(recordData);

        return new Data(fileHeader, pageHeader, freeSpace, recordData);
    }

    @Override
    public void serializeBody(ByteBuffer buffer) {
        pageHeader.serialize(buffer);
        buffer.putShort(freeSpace);
        buffer.put(recordData);
    }

    public void makeClean() {
        pageHeader.makeClean();
    }

    public void makeDirty() {
        pageHeader.makeDirty();
    }

    public boolean isDirty() {
        return pageHeader.isDirty();
    }

    public PageHeader getPageHeader() {
        return pageHeader;
    }

    public short getFreeSpace() {
        return freeSpace;
    }

    public byte[] getRecordData() {
        return recordData;
    }
}
