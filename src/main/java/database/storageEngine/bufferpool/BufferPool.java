package database.storageEngine.bufferpool;

import database.storageEngine.page.FileExtension;
import database.storageEngine.page.FileManager;
import database.storageEngine.page.Page;
import database.storageEngine.page.StorageRecord;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

public class BufferPool {

    private final int capacity;
    private final PageReplacementStrategy<TablePageKey> strategy;
    private final Map<TablePageKey, Page> pages;
    private final FileManager fileManager;

    public BufferPool(int capacity, PageReplacementStrategy<TablePageKey> strategy) {
        this.capacity = capacity;
        this.strategy = strategy;
        this.pages = new HashMap<>();
        this.fileManager = new FileManager(FileExtension.IDB);
    }

    public Optional<Page> getPage(TablePageKey key) {
        // Cache hit
        if (pages.containsKey(key)) {
            strategy.get(key);
            return Optional.of(pages.get(key));
        }

        // Cache miss
        Page page = fileManager.loadPage(key);
        putPage(key, page);
        return Optional.of(page);
    }

    public void putPage(TablePageKey key, Page page) {
        if (!pages.containsKey(key)) {
            if (pages.size() >= capacity) {
                TablePageKey evictedKey = strategy.evict();
                if (evictedKey != null) {
                    flushPage(evictedKey);
                    pages.remove(evictedKey);
                }
            }
            pages.put(key, page);
            strategy.put(key);
        }
    }

    public void flushPage(TablePageKey key) {
        if (pages.containsKey(key)) {
            Page page = pages.get(key);
            if (page.isDirty()) {
                fileManager.savePage(key.tableName(), page);
                page.clean();
            }
        }
    }

    public void flushAllPages() {
        for (Map.Entry<TablePageKey, Page> entry : pages.entrySet()) {
            flushPage(entry.getKey());
        }
    }

    public void removePage(TablePageKey key) {
        if (pages.containsKey(key)) {
            flushPage(key);
            pages.remove(key);
            strategy.evict();
        }
    }

    public Page findPageWithSpace(String tableName, StorageRecord storageRecord) {
        return IntStream.range(0, capacity)
                .mapToObj(pageNumber -> {
                    TablePageKey key = new TablePageKey(tableName, pageNumber);
                    return pages.get(key);
                })
                .filter(page -> page.getFreeSpace() >= storageRecord.getSize())
                .findFirst()
                .orElseGet(fileManager::createNewDataPage);
    }
}
