package database.storageEngine.bufferpool;

import java.util.Optional;

public interface PageReplacementStrategy<K> {

    Optional<K> get(K key);

    void put(K key);

    K evict();

    boolean contains(K key);
}
