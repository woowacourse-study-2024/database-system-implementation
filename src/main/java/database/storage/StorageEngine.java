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

    public void insertRecord(String tableName, Record record) {
        Metadata metadata = dataDictionary.getMetadata(tableName);
        int rootPageNumber = metadata.getClusteredIndex().getRootPageNumber();

        TableSpace table = new TableSpace(tableName, bufferPool);
        table.insertRecord(rootPageNumber, record);
    }
}
