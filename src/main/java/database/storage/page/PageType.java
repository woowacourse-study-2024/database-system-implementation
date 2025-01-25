package database.storage.page;

import static java.util.Arrays.stream;

public enum PageType {

    INDEX(0),
    UNDO_LOG(1);

    private final int code;

    PageType(int code) {
        this.code = code;
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
}
