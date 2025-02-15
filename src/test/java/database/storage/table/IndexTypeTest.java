package database.storage.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IndexTypeTest {

    @DisplayName("IndexType Code와 일치하는 IndexType을 반환한다.")
    @Test
    void returnsIndexTypeForValidCode() {
        assertAll(
                () -> assertThat(IndexType.fromCode(0)).isEqualTo(IndexType.CLUSTERED),
                () -> assertThat(IndexType.fromCode(1)).isEqualTo(IndexType.SECONDARY)
        );
    }


    @DisplayName("IndexType Code와 일치하는 IndexType을 찾는데 실패 시 에러를 반환한다.")
    @Test
    void throwsExceptionForInvalidCode() {
        int indexTypeCode = -1;

        assertThrows(IllegalArgumentException.class, () -> IndexType.fromCode(indexTypeCode));
    }

}
