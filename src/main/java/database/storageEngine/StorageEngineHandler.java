package database.storageEngine;

import database.engine.Handler;
import database.engine.Record;
import database.storageEngine.bufferpool.BufferPool;
import database.storageEngine.bufferpool.PageReplacementStrategy;
import database.storageEngine.bufferpool.TablePageKey;
import database.storageEngine.bufferpool.pageReplacementStrategies.LRUStrategy;
import database.storageEngine.page.Page;
import database.storageEngine.page.StorageRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StorageEngineHandler implements Handler {

    private static final int BUFFER_SIZE = 40;

    private final BufferPool bufferPool;

    public StorageEngineHandler() {
        PageReplacementStrategy<TablePageKey> lruStrategy = new LRUStrategy<>(BUFFER_SIZE);
        this.bufferPool = new BufferPool(BUFFER_SIZE, lruStrategy);
    }

    @Override
    public void insert(String tableName, Record record) {
        StorageRecord storageRecord = new StorageRecord(record.getValues());
        Page page = bufferPool.findPageWithSpace(tableName, storageRecord);
        page.addRecord(storageRecord);
    }

    @Override
    public List<Record> search(String tableName, Object key) {
        List<Record> results = new ArrayList<>();

        for (long pageNumber = 0; ; pageNumber++) {
            TablePageKey tablePageKey = new TablePageKey(tableName, pageNumber);
            Optional<Page> pageOpt = bufferPool.getPage(tablePageKey);

            if (pageOpt.isPresent()) {
                Page page = pageOpt.get();
                List<Record> records = page.searchRecords(key).stream()
                        .map(storageRecord -> new Record(storageRecord.getValues()))
                        .toList();
                if (!records.isEmpty()) {
                    results.addAll(records);
                }
            } else {
                break;
            }
        }
        return results;
    }

    @Override
    public void update(String tableName, Object key, byte[] newRecord) {
        // 디스크에서 레코드를 업데이트하는 로직
    }

    @Override
    public void delete(String tableName, Object key) {
        // 디스크에서 레코드를 삭제하는 로직
    }
}
