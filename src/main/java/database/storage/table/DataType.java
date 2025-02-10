package database.storage.table;

import static java.util.Arrays.stream;

public enum DataType {

    INT(0),
    CHAR(1),
    ;

    private final int code;

    DataType(int code) {
        this.code = code;
    }

    public static DataType fromCode(int code) {
        return stream(values())
                .filter(dataType -> dataType.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown DataType code: " + code));
    }

    public int getCode() {
        return code;
    }
}
