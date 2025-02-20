package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import database.storage.page.fspheader.ExtentState;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExtentStateTest {

    @DisplayName("ExtentState Code와 일치하는 ExtentState을 반환한다.")
    @Test
    void returnsExtentStateForValidCode() {
        assertAll(
                () -> assertThat(ExtentState.fromCode(0)).isEqualTo(ExtentState.FREE_FRAG),
                () -> assertThat(ExtentState.fromCode(1)).isEqualTo(ExtentState.FULL_FRAG),
                () -> assertThat(ExtentState.fromCode(2)).isEqualTo(ExtentState.FREE)
        );
    }


    @DisplayName("ExtentState Code와 일치하는 ExtentState을 찾는데 실패 시 에러를 반환한다.")
    @Test
    void throwsExceptionForInvalidCode() {
        int extentStateCode = -1;

        assertThrows(IllegalArgumentException.class, () -> ExtentState.fromCode(extentStateCode));
    }
}
