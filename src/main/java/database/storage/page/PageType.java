package database.storage.page;

import static java.util.Arrays.stream;

import java.nio.ByteBuffer;
import java.util.function.Function;

public enum PageType {

    DATA(0, Data::deserialize),
    FSP_HEADER(1, FspHeader::deserialize);

    private final int code;
    private final Function<ByteBuffer, Page> deserializer;

    PageType(int code, Function<ByteBuffer, Page> deserializer) {
        this.code = code;
        this.deserializer = deserializer;
    }

    public static PageType fromCode(int code) {
        return stream(values())
                .filter(pageType -> pageType.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown PageType code: " + code));
    }

    public int getCode() {
        return code;
    }

    public Function<ByteBuffer, Page> getDeserializer() {
        return deserializer;
    }
}
