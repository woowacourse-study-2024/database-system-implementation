package database.storageEngine.page;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Page implements Serializable {

    private static final int PAGE_SIZE = 16 * 1024;

    private final FileHeader fileHeader;
    private final PageHeader pageHeader;
    private final List<StorageRecord> userStorageRecords;
    private final PageDirectory pageDirectory;
    private int freeSpace;

    public Page(long pageNumber, PageType pageType) {
        this.fileHeader = new FileHeader(pageNumber);
        this.pageHeader = new PageHeader(pageType);
        this.userStorageRecords = new ArrayList<>();
        this.pageDirectory = new PageDirectory();

        int initialDirectorySize = pageDirectory.getSize();
        this.freeSpace = getFreeSpace(initialDirectorySize);
    }

    private int getFreeSpace(int initialDirectorySize) {
        return PAGE_SIZE - (this.fileHeader.getHeaderSize() + this.pageHeader.getHeaderSize() + initialDirectorySize);
    }

    public boolean addRecord(StorageRecord storageRecord) {
        int recordSize = storageRecord.getSize();
        int directorySizeIncrease = Integer.BYTES;

        if (freeSpace >= recordSize + directorySizeIncrease) {
            userStorageRecords.add(storageRecord);
            pageDirectory.addRecordPosition(userStorageRecords.size() - 1);
            freeSpace -= (recordSize + directorySizeIncrease);
            pageHeader.incrementRecordCount();
            markDirty();
            return true;
        } else {
            return false;
        }
    }

    public void markDirty() {
        pageHeader.markDirty();
    }

    public boolean isDirty() {
        return pageHeader.isDirty();
    }

    public void clean() {
        pageHeader.clean();
    }

    public List<StorageRecord> searchRecords(Object key) {
        return userStorageRecords.stream()
                .filter(storageRecord -> storageRecord.contains(key))
                .toList();
    }

    public long getPageNumber() {
        return fileHeader.getPageNumber();
    }

    public PageType getPageType() {
        return pageHeader.getPageType();
    }

    public int getRecordCount() {
        return pageHeader.getRecordCount();
    }

    public int getFreeSpace() {
        return freeSpace;
    }

    @Override
    public String toString() {
        return "Page{" +
                "fileHeader=" + fileHeader +
                ", pageHeader=" + pageHeader +
                ", userStorageRecords=" + userStorageRecords +
                ", pageDirectory=" + pageDirectory +
                ", freeSpace=" + freeSpace +
                '}';
    }
}
