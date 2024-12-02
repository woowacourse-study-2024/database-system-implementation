package database.storageEngine.bufferpool.pageReplacementStrategies;

import database.storageEngine.bufferpool.PageReplacementStrategy;
import java.util.LinkedHashSet;
import java.util.Optional;

public class LRUStrategy<K> implements PageReplacementStrategy<K> {

    private final int capacity;
    private final LinkedHashSet<K> cache;

    public LRUStrategy(int capacity) {
        this.capacity = capacity;
        this.cache = new LinkedHashSet<>(capacity);
    }

    @Override
    public void put(K key) {
        if (cache.contains(key)) {
            cache.remove(key);
        } else if (cache.size() >= capacity) {
            evict();
        }
        cache.add(key);
    }

    @Override
    public K evict() {
        K firstKey = cache.iterator().next();
        cache.remove(firstKey);
        return firstKey;
    }

    @Override
    public boolean contains(K key) {
        return cache.contains(key);
    }

    @Override
    public Optional<K> get(K key) {
        if (cache.contains(key)) {
            cache.remove(key);
            cache.add(key);
            return Optional.of(key);
        }
        return Optional.empty();
    }
}

