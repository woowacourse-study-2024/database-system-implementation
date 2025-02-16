package database.storage.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LRUStrategyTest {

    @DisplayName("데이터 추가 시 캐시 용량을 초과하면 가장 오래된 데이터가 캐시에서 제거된다.")
    @Test
    void removeEldestEntryTest() {
        int capacity = 3;
        PageReplacementStrategy<Integer> cache = new LRUStrategy<>(capacity);
        cache.put(1);
        cache.put(2);
        cache.put(3);

        cache.put(4);

        assertAll(
                () -> assertThat(cache.contains(1)).isFalse(),
                () -> assertThat(cache.contains(4)).isTrue()
        );
    }

    @DisplayName("데이터 만료 시 가장 오래된 데이터가 캐시에서 제거된다.")
    @Test
    void evictTest() {
        int capacity = 3;

        PageReplacementStrategy<Integer> cache = new LRUStrategy<>(capacity);
        cache.put(1);
        cache.put(2);
        cache.put(3);

        Integer evicted = cache.evict();

        assertAll(
                () -> assertThat(evicted).isEqualTo(1),
                () -> assertThat(cache.contains(evicted)).isFalse()
        );
    }
}
