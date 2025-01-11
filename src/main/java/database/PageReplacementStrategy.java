package database;

import java.util.Collection;

public interface PageReplacementStrategy<K> {

    void access(K key);

    void put(K key);

    K evict();

    boolean remove(K key);

    boolean contains(K key);

    Collection<K> keySet();
}
