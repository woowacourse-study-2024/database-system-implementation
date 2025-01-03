package database.engine.page;

import java.util.ArrayList;
import java.util.List;

public class UserRecords {

    private final List<Record> records;

    public UserRecords() {
        this.records = new ArrayList<>();
    }

    public List<Record> getRecords() {
        return records;
    }
}
