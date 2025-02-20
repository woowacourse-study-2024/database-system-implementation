package database.storage;

import database.storage.datadictionary.Metadata;
import database.storage.record.Record;

public class StorageEngine {

    private final BufferPool bufferPool;
    private final DataDictionary dataDictionary;

    public StorageEngine(BufferPool bufferPool, DataDictionary dataDictionary) {
        this.bufferPool = bufferPool;
        this.dataDictionary = dataDictionary;
    }

    public void insertRecords(String tableName, Record record) {
        Metadata metadata = dataDictionary.getMetadata(tableName);
        int rootPageNumber = metadata.getClusteredIndex().getRootPageNumber();

        TableSpace table = new TableSpace(tableName, bufferPool);
        table.insertRecords(rootPageNumber, record);
    }

    public <T> RecordSet<T> selectRecords(String tableName, Condition condition) {
        Metadata metadata = dataDictionary.getMetadata(tableName);
        int rootPageNumber = metadata.getClusteredIndex().getRootPageNumber();

        TableSpace table = new TableSpace(tableName, bufferPool);
        return table.selectRecords(rootPageNumber, condition);
    }

    // Undo Log 및 MVCC 미구현
    public void deleteRecords(String tableName, Condition condition) {
        Metadata metadata = dataDictionary.getMetadata(tableName);
        int rootPageNumber = metadata.getClusteredIndex().getRootPageNumber();

        TableSpace table = new TableSpace(tableName, bufferPool);
        table.deleteRecords(rootPageNumber, condition);
    }
}
