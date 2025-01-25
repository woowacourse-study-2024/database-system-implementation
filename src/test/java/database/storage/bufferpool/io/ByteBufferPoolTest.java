package database.storage.bufferpool.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ByteBufferPoolTest {

    private final long maxTimeToBlockMs = 10;

    @DisplayName("버퍼 크기는 양수여야 한다.")
    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testBufferSizeMustBePositive(int bufferSize) {
        int totalMemory = 1024;

        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(totalMemory, bufferSize));
    }

    @DisplayName("총 메모리 크기는 양수여야 한다.")
    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testTotalMemorySizeMustBePositive(int totalMemory) {
        int bufferSize = 16;

        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(totalMemory, bufferSize));
    }

    @DisplayName("총 메모리 크기는 버퍼 크기의 배수여야 한다.")
    @ParameterizedTest
    @ValueSource(ints = {3, 15, 17})
    void testTotalMemoryMustBeMultipleOfBufferSize(int bufferSize) {
        int totalMemory = 1024;

        assertThrows(IllegalArgumentException.class, () -> new ByteBufferPool(totalMemory, bufferSize));
    }

    @DisplayName("버퍼 풀을 정상적으로 생성한다.")
    @Test
    void testBufferPoolCreation() {
        int bufferSize = 16;
        int totalMemory = bufferSize * 64;

        ByteBufferPool pool = new ByteBufferPool(totalMemory, bufferSize);

        assertThat(pool.freeSize()).isEqualTo(64);
    }

    @DisplayName("동시성 할당 테스트")
    @Test
    void testConcurrentAllocation() throws InterruptedException {
        ByteBufferPool pool = new ByteBufferPool(16, 16);
        ByteBuffer buffer = pool.allocate(maxTimeToBlockMs);

        CountDownLatch doDeallocate = asyncDeallocate(pool, buffer);
        CountDownLatch allocation = asyncAllocate(pool);

        assertThat(allocation.getCount()).isEqualTo(1L);
        doDeallocate.countDown();
        assertThat(allocation.await(1, TimeUnit.SECONDS)).isTrue();
    }

    @DisplayName("할당 과정에서 사용 가능한 메모리가 없는 경우 최대 시간만큼 대기하다가 예외가 발생한다.")
    @Test
    void testDelayedAllocation() {
        ByteBufferPool pool = new ByteBufferPool(1, 1);
        ByteBuffer buffer = pool.allocate(maxTimeToBlockMs);

        assertThrows(IllegalStateException.class, () -> pool.allocate(maxTimeToBlockMs));
    }

    @DisplayName("할당 해제 과정에서 지연이 발생하여 할당 최대 시간을 초과하는 경우 예외가 발생한다.")
    @Test
    void testDelayedDeallocate() {
        ByteBufferPool pool = new ByteBufferPool(3, 1);
        ByteBuffer buffer1 = pool.allocate(maxTimeToBlockMs);
        ByteBuffer buffer2 = pool.allocate(maxTimeToBlockMs);
        ByteBuffer buffer3 = pool.allocate(maxTimeToBlockMs);

        delayedDeallocate(pool, buffer1, maxTimeToBlockMs * 10);
        delayedDeallocate(pool, buffer2, maxTimeToBlockMs * 20);
        delayedDeallocate(pool, buffer3, maxTimeToBlockMs * 30);

        assertThrows(IllegalStateException.class, () -> pool.allocate(maxTimeToBlockMs));
    }

    private CountDownLatch asyncDeallocate(ByteBufferPool pool, ByteBuffer buffer) {
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            pool.deallocate(buffer);
        });
        thread.start();
        return latch;
    }

    private CountDownLatch asyncAllocate(ByteBufferPool pool) {
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                pool.allocate(maxTimeToBlockMs);
            } finally {
                latch.countDown();
            }
        });
        thread.start();
        return latch;
    }

    private void delayedDeallocate(ByteBufferPool pool, ByteBuffer buffer, long delayMs) {
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(delayMs);
                pool.deallocate(buffer);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }
}
