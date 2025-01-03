package database.engine.page;

public class Page {

    public static final int PAGE_SIZE = 16 * 1024;

    private final FileHeader fileHeader;
    private final PageHeader pageHeader;
    private final UserRecords userRecords;
    private int freeSpace;

    public Page(FileHeader fileHeader, PageHeader pageHeader, UserRecords userRecords, int freeSpace) {
        this.fileHeader = fileHeader;
        this.pageHeader = pageHeader;
        this.userRecords = userRecords;
        this.freeSpace = freeSpace;
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
