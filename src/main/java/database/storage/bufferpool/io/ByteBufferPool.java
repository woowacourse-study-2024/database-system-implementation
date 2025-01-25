package database.storage.bufferpool.io;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ByteBufferPool {

    private final int bufferSize;
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    private boolean closed;

    public ByteBufferPool(long totalMemory, int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be positive.");
        }
        if (totalMemory <= 0) {
            throw new IllegalArgumentException("Total memory must be positive.");
        }
        if (totalMemory % bufferSize != 0) {
            throw new IllegalArgumentException("Total memory must be a multiple of buffer size.");
        }

        this.bufferSize = bufferSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.closed = false;
        preAllocateBuffers(totalMemory, bufferSize);
    }

    private void preAllocateBuffers(long totalMemory, int bufferSize) {
        int maxBuffers = (int) (totalMemory / bufferSize);
        for (int i = 0; i < maxBuffers; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            free.addLast(buffer);
        }
    }

    public ByteBuffer allocate(long maxTimeToBlockMs) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Producer closed while allocating memory");
            }

            if (!free.isEmpty()) {
                return free.pollFirst();
            }

            Condition waiter = lock.newCondition();
            waiters.addLast(waiter);

            try {
                while (true) {
                    boolean waitingTimeElapsed = !waiter.await(maxTimeToBlockMs, TimeUnit.MILLISECONDS);

                    if (closed) {
                        throw new IllegalStateException("Producer closed while allocating memory");
                    }
                    if (waitingTimeElapsed) {
                        throw new IllegalStateException(
                                "Failed To allocate buffer within the configured max blocking time "
                                        + maxTimeToBlockMs + " ms."
                        );
                    }

                    if (!free.isEmpty()) {
                        return free.pollFirst();
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                waiters.remove(waiter);
            }
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer to deallocate cannot be null");
        }
        if (buffer.capacity() != bufferSize) {
            throw new IllegalArgumentException("Buffer size " + buffer.capacity()
                    + " does not match the pool's buffer size of " + bufferSize + " bytes.");
        }

        lock.lock();
        try {
            buffer.clear();
            free.addLast(buffer);

            if (!waiters.isEmpty()) {
                Condition waiter = waiters.pollFirst();
                waiter.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            waiters.forEach(Condition::signal);
        } finally {
            lock.unlock();
        }
    }

    // for testing.
    int freeSize() {
        return free.size();
    }
}
