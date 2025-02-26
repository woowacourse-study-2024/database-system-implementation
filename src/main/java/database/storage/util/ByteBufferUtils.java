package database.storage.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteBufferUtils {

    private static final String EMPTY_STRING = "";

    public static String readString(ByteBuffer buffer) {
        byte length = buffer.get();
        if (length == 0) {
            return EMPTY_STRING;
        }
        if (length < 0) {
            throw new IllegalArgumentException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
