package database.engine.page;

public class Record {

    private final int recordId;
    private final byte[] data;

    public Record(int recordId, byte[] data) {
        this.recordId = recordId;
        this.data = data;
    }

    public int getRecordId() {
        return recordId;
    }

    public byte[] getData() {
        return data;
    }
}
