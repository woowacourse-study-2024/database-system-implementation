package database.storage.datadictionary;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Column {

    private static final int TRUE = 1;
    private static final int FALSE = 0;

    private final String name;
    private final DataType type;
    private final boolean nullable;
    private final short length;

    public Column(String name, DataType type, boolean nullable, short length) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.length = length;
    }

    public static Column deserialize(ByteBuffer buffer) {
        String name = ByteBufferUtils.readString(buffer);
        DataType dataType = DataType.fromCode(buffer.get());
        boolean nullable = buffer.get() == TRUE;
        short length = buffer.getShort();

        return new Column(name, dataType, nullable, length);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.put((byte) name.length());
        buffer.put(name.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) type.getCode());
        buffer.put((byte) (nullable ? TRUE : FALSE));
        buffer.putShort(length);
    }

    public boolean hasName(String other) {
        return name.equals(other);
    }

    public int getByteSize() {
        return 1 + name.length() + 1 + 1 + 2;
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public short getLength() {
        return length;
    }
}
