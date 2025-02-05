package database.storage;

import java.nio.ByteBuffer;

public class BaseNode {

    public static final int SIZE = Pointer.SIZE * 2;

    private Pointer first;
    private Pointer last;

    public BaseNode(Pointer first, Pointer last) {
        this.first = first;
        this.last = last;
    }

    public static BaseNode deserialize(ByteBuffer buffer) {
        Pointer first = Pointer.deserialize(buffer);
        Pointer last = Pointer.deserialize(buffer);

        return new BaseNode(first, last);
    }

    public void serialize(ByteBuffer buffer) {
        first.serialize(buffer);
        last.serialize(buffer);
    }

    public Pointer getFirst() {
        return first;
    }

    public Pointer getLast() {
        return last;
    }
}
