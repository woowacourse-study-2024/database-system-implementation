package database.engine.page;

import java.nio.ByteBuffer;

public class Page {

    public static final short PAGE_SIZE = 16 * 1024;

    private final FileHeader fileHeader;
    private final PageHeader pageHeader;
    private final UserRecords userRecords;
    private short freeSpace;

    private Page(FileHeader fileHeader, PageHeader pageHeader, UserRecords userRecords, short freeSpace) {
        this.fileHeader = fileHeader;
        this.pageHeader = pageHeader;
        this.userRecords = userRecords;
        this.freeSpace = freeSpace;
    }

    public static Page deserialize(ByteBuffer buffer) {
        FileHeader fileHeader = FileHeader.deserialize(buffer);
        PageHeader pageHeader = PageHeader.deserialize(buffer);
        UserRecords userRecords = UserRecords.deserialize(buffer, pageHeader.getRecordCount());
        short freeSpace = buffer.getShort();

        return new Page(fileHeader, pageHeader, userRecords, freeSpace);
    }

    public FileHeader getFileHeader() {
        return fileHeader;
    }

    public PageHeader getPageHeader() {
        return pageHeader;
    }

    public UserRecords getUserRecords() {
        return userRecords;
    }

    public int getFreeSpace() {
        return freeSpace;
    }
}
