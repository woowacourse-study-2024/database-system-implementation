package database.storage.datadictionary;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Index {

    private final String name;
    private final IndexType type;
    private final int rootPageNumber;

    public Index(String name, IndexType type, int rootPageNumber) {
        this.name = name;
        this.type = type;
        this.rootPageNumber = rootPageNumber;
    }

    public static Index deserialize(ByteBuffer buffer) {
        String name = ByteBufferUtils.readString(buffer);
        IndexType type = IndexType.fromCode(buffer.get());
        int rootPageNumber = buffer.getInt();

        return new Index(name, type, rootPageNumber);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.put((byte) name.length());
        buffer.put(name.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) type.getCode());
        buffer.putInt(rootPageNumber);
    }

    public boolean isClustered() {
        return type == IndexType.CLUSTERED;
    }

    public boolean hasName(String other) {
        return name.equals(other);
    }

    public int getByteSize() {
        return 1 + name.length() + 1 + 4;
    }

    public String getName() {
        return name;
    }

    public IndexType getType() {
        return type;
    }

    public int getRootPageNumber() {
        return rootPageNumber;
    }
}
