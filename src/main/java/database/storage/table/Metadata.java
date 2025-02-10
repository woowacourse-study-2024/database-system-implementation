package database.storage.table;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Metadata {

    private final long tableId;
    private final String tableName;
    private final int columnCount;
    private final Columns columns;

    public Metadata(long tableId, String tableName, int columnCount, Columns columns) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.columnCount = columnCount;
        this.columns = columns;
    }

    public static Metadata deserialize(ByteBuffer buffer) {
        long tableId = buffer.getLong();
        String tableName = ByteBufferUtils.readString(buffer);
        int columnCount = buffer.getInt();
        Columns columns = Columns.deserialize(buffer, columnCount);

        return new Metadata(tableId, tableName, columnCount, columns);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putLong(tableId);
        buffer.put((byte) tableName.length());
        buffer.put(tableName.getBytes(StandardCharsets.UTF_8));
        buffer.putInt(columnCount);
        columns.serialize(buffer);
    }

    public boolean isSameTableName(String tableName) {
        return this.tableName.equalsIgnoreCase(tableName);
    }

    public int getByteSize() {
        return 8 + (1 + tableName.length()) + 4 + columns.getByteSize();
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public Columns getColumns() {
        return columns;
    }
}
