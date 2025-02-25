package database.storage.datadictionary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

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

    public Column getColumn(String columnName) {
        return columns.stream()
                .filter(column -> column.hasName(columnName))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Column not found: " + columnName));
    }

    public List<Column> getColumns() {
        return columns;
    }
}
