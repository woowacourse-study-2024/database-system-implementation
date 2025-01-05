package database.engine.page;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

public class UserRecords {

    private final List<Record> records;

    private UserRecords(List<Record> records) {
        this.records = records;
    }

    public static UserRecords deserialize(ByteBuffer buffer, short recordCount) {
        List<Record> records = new LinkedList<>(deserializeRecords(buffer, recordCount));
        return new UserRecords(records);
    }

    private static List<Record> deserializeRecords(ByteBuffer buffer, short recordCount) {
        List<Record> records = new LinkedList<>();

        IntStream.range(0, recordCount)
                .forEach(i -> {
                    int currentPosition = buffer.position();
                    Record record = Record.deserialize(buffer, currentPosition);
                    records.add(record);
                    buffer.position(record.getNextRecordOffset());
                });

        return records;
    }

    public List<Record> getRecords() {
        return records;
    }
}
