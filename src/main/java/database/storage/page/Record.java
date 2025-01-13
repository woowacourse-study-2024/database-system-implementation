package database.storage.page;

import java.nio.ByteBuffer;

public class Record {

    private final short nextRecordOffset;
    private final byte[] data;

    private Record(short nextRecordOffset, byte[] data) {
        this.nextRecordOffset = nextRecordOffset;
        this.data = data;
    }

    public static Record deserialize(ByteBuffer buffer, int currentPosition) {
        short nextRecordOffset = buffer.getShort();
        int dataLength = nextRecordOffset - currentPosition - 2;
        byte[] data = new byte[dataLength];
        buffer.get(data);

        return new Record(nextRecordOffset, data);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putShort(nextRecordOffset);
        buffer.put(data);
    }

    public short getNextRecordOffset() {
        return nextRecordOffset;
    }

    public byte[] getData() {
        return data;
    }
}
