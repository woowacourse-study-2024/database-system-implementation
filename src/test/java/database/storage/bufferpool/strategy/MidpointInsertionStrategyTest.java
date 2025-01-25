package database.storage.bufferpool.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MidpointInsertionStrategyTest {

    @DisplayName("데이터가 임계 값이상 조회되면 MRU 영역 head로 이동시킨다.")
    @Test
    void testMoveToHead() {
        MidpointInsertionStrategy<Integer> strategy = new MidpointInsertionStrategy<>(3, 2, 30);

        strategy.put(1);
        strategy.put(2);
        strategy.put(3);

        strategy.access(3);
        assertThat(strategy.getHead() == 3).isFalse();

        strategy.access(3);
        assertThat(strategy.getHead() == 3).isTrue();
    }

    @DisplayName("현재 캐시가 old sublist 크기보다 작으면 새로운 데이터는 LRU 영역 head에 삽입된다.")
    @Test
    void testTailInsertionWhenSizeLessThanOrEqualOldSublist() {
        MidpointInsertionStrategy<Integer> strategy = new MidpointInsertionStrategy<>(3, 2, 60);

        strategy.put(1);
        strategy.put(2);

        assertThat(strategy.getTail() == 1).isTrue();
    }

    @DisplayName("현재 캐시가 old sublist 크기보다 크면 새로운 데이터는 new sublist와 old sublist 사이에 삽입된다.")
    @Test
    void testInsertAtMidpointWhenSizeGreaterThanOldSublist() {
        MidpointInsertionStrategy<Integer> strategy = new MidpointInsertionStrategy<>(5, 2, 40);

        strategy.put(1);
        strategy.put(2);
        strategy.put(3);
        assertThat(strategy.getHead()).isEqualTo(3);

        strategy.put(4);
        assertThat(strategy.getHead()).isEqualTo(3);
    }

    @DisplayName("대량 삽입중에도 자주 사용된 페이지가 밀려나지 않는다.")
    @Test
    void testScanResistance() {
        // capacity=5, thresholdHits=2, oldSublistPercent=40 (oldSublistSize≈2)
        MidpointInsertionStrategy<Integer> strategy = new MidpointInsertionStrategy<>(5, 2, 40);

        strategy.put(99); // [head=tail=99]
        strategy.put(98); // [98 head, 99 tail]
        strategy.put(97); // [97 head, 98, 99 tail]

        // HOT PAGE 다중 접근 -> MRU List로 승격
        for (int i = 0; i < 3; i++) {
            strategy.access(99);
            strategy.access(98);
        }

        // 다량의 페이지 연속 삽입
        strategy.put(1); // [98 head, 99, 1, 97 tail]
        strategy.put(2); // [98 head, 99, 1, 2, 97 tail]
        strategy.put(3); // [98 head, 99, 1, 3, 2 tail] evict: 97
        strategy.put(4); // [98 head, 99, 1, 4, 3 tail] evict: 2
        strategy.put(5); // [98 head, 99, 1, 5, 4 tail] evict: 3
        strategy.put(6); // [98 head, 99, 1, 6, 5 tail] evict: 4
        strategy.put(7); // [98 head, 99, 1, 7, 6 tail] evict: 5

        assertTrue(strategy.contains(99), "핫 페이지 99는 캐시에 남는다.");
        assertTrue(strategy.contains(98), "핫 페이지 98는 캐시에 남는다.");
        assertFalse(strategy.contains(97), "페이지 97는 캐시에 남지 않는다.");
    }
}
