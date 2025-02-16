package database.storage;

import database.storage.bufferpool.BufferPool;
import database.storage.page.Record;
import database.storage.table.Metadata;

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
