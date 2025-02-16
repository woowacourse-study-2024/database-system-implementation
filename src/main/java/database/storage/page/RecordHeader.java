package database.storage.page;

public class RecordHeader {

    public static final int HEADER_SIZE = 1 + 2 + 1;

    private final RecordType recordType;
    private final short nextOffset;
    private boolean isDeleted;

    private RecordHeader(RecordType recordType, short nextOffset, boolean isDeleted) {
        this.recordType = recordType;
        this.nextOffset = nextOffset;
        this.isDeleted = isDeleted;
    }

    public void delete() {
        isDeleted = true;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public short getNextOffset() {
        return nextOffset;
    }

    public boolean isDeleted() {
        return isDeleted;
    }
}
