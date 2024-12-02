package database.storageEngine.bufferpool.pageReplacementStrategies;

import database.storageEngine.bufferpool.PageReplacementStrategy;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public class LFUStrategy<K> implements PageReplacementStrategy<K> {

    private final int capacity;
    private final Map<K, Integer> usageCount;
    private final Map<K, Long> cache;

    public LFUStrategy(int capacity) {
        this.capacity = capacity;
        this.usageCount = new HashMap<>();
        this.cache = new HashMap<>();
    }

    @Override
    public void put(K key) {
        if (cache.size() >= capacity) {
            evict();
        }
        cache.put(key, System.nanoTime()); // 현재 시간으로 타임스탬프 저장
        usageCount.put(key, usageCount.getOrDefault(key, 0) + 1);
    }

    @Override
    public K evict() {
        K leastUsedKey = findLeastUsedKey();
        if (leastUsedKey != null) {
            cache.remove(leastUsedKey);
            usageCount.remove(leastUsedKey);
        }
        return leastUsedKey;
    }

    @Override
    public boolean contains(K key) {
        return cache.containsKey(key);
    }

    private K findLeastUsedKey() {
        Optional<Entry<K, Integer>> minEntry = usageCount.entrySet().stream()
                .min((entry1, entry2) -> {
                    int freqCompare = entry1.getValue().compareTo(entry2.getValue());
                    if (freqCompare == 0) {
                        // 동일한 사용 빈도일 경우, 먼저 들어온 키를 우선 제거
                        return cache.get(entry1.getKey()).compareTo(cache.get(entry2.getKey()));
                    }
                    return freqCompare;
                });
        return minEntry.map(Map.Entry::getKey).orElse(null);
    }

    @Override
    public Optional<K> get(K key) {
        if (cache.containsKey(key)) {
            usageCount.put(key, usageCount.get(key) + 1);
            return Optional.of(key);
        }
        return Optional.empty();
    }
}
