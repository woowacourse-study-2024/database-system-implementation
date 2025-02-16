package database.storage.datadictionary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Columns {

    private final List<Column> columns;

    public Columns(List<Column> columns) {
        this.columns = columns;
    }

    public static Columns deserialize(ByteBuffer buffer, int columnCount) {
        List<Column> columns = new ArrayList<>();

        for (int i = 0; i < columnCount; i++) {
            columns.add(Column.deserialize(buffer));
        }

        return new Columns(columns);
    }

    public void serialize(ByteBuffer buffer) {
        columns.forEach(column -> column.serialize(buffer));
    }

    public int getByteSize() {
        return columns.stream()
                .mapToInt(Column::getByteSize)
                .sum();
    }

    public List<Column> getColumns() {
        return columns;
    }
}
