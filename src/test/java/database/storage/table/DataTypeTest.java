package database.storage.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DataTypeTest {

    @DisplayName("dataTypeCode와 일치하는 DataType을 반환한다.")
    @Test
    void returnsDataTypeForValidCode() {
        assertAll(
                () -> assertThat(DataType.fromCode(0)).isEqualTo(DataType.INT),
                () -> assertThat(DataType.fromCode(1)).isEqualTo(DataType.CHAR)
        );
    }


    @DisplayName("dataTypeCode와 일치하는 DataType을 찾는데 실패 시 에러를 반환한다.")
    @Test
    void throwsExceptionForInvalidCode() {
        int pageTypeCode = -1;

        assertThrows(IllegalArgumentException.class, () -> DataType.fromCode(pageTypeCode));
    }
}
