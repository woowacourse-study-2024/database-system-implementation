package database.storage.strategy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * MidpointInsertionStrategy
 * head: MRU 영역
 * tail: LRU 영역
 * 새로 들어온 데이터는 midPoint 에 삽입
 * threshHoldHits 이상 hit 되면 MRU 의 head 로 승격
 * 자주 참조되지 않는 페이지는 LRU 의 tail 로 점점 밀려나고 제거
 * [head - new sublist(MRU) - tail]-midPoint-[head - new sublist(LRU) - tail]
 * [head <- (prev K next) -> ( ) ... tail]
 */

public class MidpointInsertionStrategy<K> implements PageReplacementStrategy<K> {

    private final Map<K, Node> cache;
    private final int capacity;
    private final int oldSublistSize;
    private final int thresholdHits;
    private Node head;
    private Node tail;

    public MidpointInsertionStrategy(int capacity, int thresholdHits, int oldSublistPercent) {
        this.cache = new HashMap<>();
        this.capacity = capacity;
        this.thresholdHits = thresholdHits;
        this.oldSublistSize = Math.round((float) (capacity * oldSublistPercent) / 100);
        this.head = null;
        this.tail = null;
    }

    @Override
    public void access(K key) {
        if (!cache.containsKey(key)) {
            return;
        }

        Node node = cache.get(key);
        node.accessCount++;

        if (node.accessCount >= thresholdHits) {
            moveToHead(node);
        }
    }

    @Override
    public void put(K key) {
        if (cache.containsKey(key)) {
            access(key);
            return;
        }

        if (cache.size() >= capacity) {
            evict();
        }

        Node node = new Node(key);
        insertAtMidPoint(node);

        cache.put(key, node);
    }

    @Override
    public K evict() {
        if (tail == null) {
            return null;
        }

        K key = tail.key;
        cache.remove(key);
        removeNode(tail);
        return key;
    }

    @Override
    public boolean remove(K key) {
        if (!cache.containsKey(key)) {
            return false;
        }

        Node node = cache.get(key);
        removeNode(node);
        cache.remove(key);
        return true;
    }

    @Override
    public boolean contains(K key) {
        return cache.containsKey(key);
    }

    @Override
    public boolean shouldEvict() {
        return cache.size() >= capacity;
    }

    @Override
    public Collection<K> keySet() {
        return cache.keySet();
    }

    private void moveToHead(Node node) {
        if (node == head) {
            return;
        }

        removeNode(node);
        addToHead(node);
    }

    private void removeNode(Node node) {
        Node prev = node.prev;
        Node next = node.next;

        if (prev != null) {
            prev.next = next;
        } else {
            head = next;
        }

        if (next != null) {
            next.prev = prev;
        } else {
            tail = prev;
        }
    }

    private void addToHead(Node node) {
        node.prev = null;
        node.next = head;

        if (head != null) {
            head.prev = node;
        }
        head = node;

        if (tail == null) {
            tail = node;
        }
    }

    private void insertAtMidPoint(Node node) {
        if (cache.size() <= oldSublistSize) {
            addToHead(node);
            return;
        }

        Node current = tail;
        for (int i = 1; i < oldSublistSize - 1; i++) {
            current = current.prev;
        }

        if (current == null || current.prev == null) {
            addToHead(node);
            return;
        }

        node.prev = current.prev;
        node.next = current;
        current.prev.next = node;
        current.prev = node;
    }

    private class Node {
        private final K key;
        private Node prev, next;
        private int accessCount;

        private Node(K key) {
            this.key = key;
            this.accessCount = 0;
        }
    }

    // for testing
    K getHead() {
        return head.key;
    }

    // for testing
    K getTail() {
        return tail.key;
    }
}
