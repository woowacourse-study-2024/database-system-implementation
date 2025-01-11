package database;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public class LRUStrategy<K> implements PageReplacementStrategy<K> {

    private final Set<K> cache;
    private final int capacity;

    public LRUStrategy(int capacity) {
        this.cache = new LinkedHashSet<>();
        this.capacity = capacity;
    }

    @Override
    public void access(K key) {
        if (cache.remove(key)) {
            cache.add(key);
        }
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
        K first = cache.iterator().next();
        cache.remove(first);
        return first;
    }

    @Override
    public boolean remove(K key) {
        return cache.remove(key);
    }

    @Override
    public boolean contains(K key) {
        return cache.contains(key);
    }

    @Override
    public Collection<K> keySet() {
        return cache;
    }
}
