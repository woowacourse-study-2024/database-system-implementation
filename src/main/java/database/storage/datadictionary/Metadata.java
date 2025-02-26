package database.storage.datadictionary;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Metadata {

    private final long tableId;
    private final String tableName;
    private final byte columnCount;
    private final Columns columns;
    private final byte indexCount;
    private final Indexes indexes;

    public Metadata(
            long tableId,
            String tableName,
            byte columnCount,
            Columns columns,
            byte indexCount,
            Indexes indexes
    ) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.columnCount = columnCount;
        this.columns = columns;
        this.indexCount = indexCount;
        this.indexes = indexes;
    }

    public static Metadata deserialize(ByteBuffer buffer) {
        long tableId = buffer.getLong();
        String tableName = ByteBufferUtils.readString(buffer);
        byte columnCount = buffer.get();
        Columns columns = Columns.deserialize(buffer, columnCount);
        byte indexCount = buffer.get();
        Indexes indexes = Indexes.deserialize(buffer, indexCount);

        return new Metadata(tableId, tableName, columnCount, columns, indexCount, indexes);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putLong(tableId);
        buffer.put((byte) tableName.length());
        buffer.put(tableName.getBytes(StandardCharsets.UTF_8));
        buffer.put(columnCount);
        columns.serialize(buffer);
        buffer.put(indexCount);
        indexes.serialize(buffer);
    }

    public Index getClusteredIndex() {
        return indexes.getClustered();
    }

    public Index getIndex(String indexName) {
        return indexes.getIndex(indexName);
    }

    public Column getColumn(String columnName) {
        return columns.getColumn(columnName);
    }

    public int getByteSize() {
        return 8 + (1 + tableName.length()) + (1 + columns.getByteSize()) + (1 + indexes.getByteSize());
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public byte getColumnCount() {
        return columnCount;
    }

    public Columns getColumns() {
        return columns;
    }

    public byte getIndexCount() {
        return indexCount;
    }
}
