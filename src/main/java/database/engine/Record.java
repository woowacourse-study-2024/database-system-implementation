package database.engine;

import java.util.List;

public class Record {

    private final List<Object> values;

    public Record(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Record{" +
                "values=" + values +
                '}';
    }
}
