package database.storage.record;

import java.util.List;

public class Record {

    private final RecordHeader recordHeader;
    private final short length;
    private final List<Field> key;
    private final List<Field> row;

    private Record(RecordHeader recordHeader, short length, List<Field> key, List<Field> row) {
        this.recordHeader = recordHeader;
        this.length = length;
        this.key = key;
        this.row = row;
    }

    public RecordHeader getRecordHeader() {
        return recordHeader;
    }

    public short getLength() {
        return length;
    }

    public List<Field> getKey() {
        return key;
    }

    public List<Field> getRow() {
        return row;
    }
}
