package database.storage.page.fspheader;

import static java.util.Arrays.stream;

public enum ExtentState {

    FREE_FRAG(0),
    FULL_FRAG(1),
    FREE(2),
    ;

    private final int code;

    ExtentState(int code) {
        this.code = code;
    }

    public static ExtentState fromCode(int code) {
        return stream(values())
                .filter(extentState -> extentState.code == code)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown ExtentType code: " + code));
    }

    public int getCode() {
        return code;
    }
}
