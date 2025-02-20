package database.storage.record;

import static java.util.Arrays.stream;

public enum RecordType {

    CONVENTIONAL(0),
    NODE_POINTER(1),
    INFIMUM(2),
    SUPREMUM(3),
    ;

    private final int code;

    RecordType(int code) {
        this.code = code;
    }

    public static RecordType fromCode(int code) {
        return stream(values())
                .filter(recordType -> recordType.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown RecordType code: " + code));
    }

    public int getCode() {
        return code;
    }
}
