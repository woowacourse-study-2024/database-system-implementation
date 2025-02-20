package database.storage.datadictionary;

import static java.util.Arrays.stream;

public enum IndexType {

    CLUSTERED(0),
    SECONDARY(1),
    ;

    private final int code;

    IndexType(int code) {
        this.code = code;
    }

    public static IndexType fromCode(int code) {
        return stream(values())
                .filter(indexType -> indexType.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown IndexType code: " + code));
    }

    public int getCode() {
        return code;
    }
}
