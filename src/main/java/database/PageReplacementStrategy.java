package database;

public interface PageReplacementStrategy<K> {

    void access(K key);

    void put(K key);

    K evict();

    boolean contains(K key);
}
