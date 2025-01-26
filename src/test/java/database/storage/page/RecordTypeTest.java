package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RecordTypeTest {

    @DisplayName("RecordType Code와 일치하는 RecordType을 반환한다.")
    @Test
    void returnsRecordTypeForValidCode() {
        assertAll(
                () -> assertThat(RecordType.fromCode(0)).isEqualTo(RecordType.CONVENTIONAL),
                () -> assertThat(RecordType.fromCode(1)).isEqualTo(RecordType.NODE_POINTER),
                () -> assertThat(RecordType.fromCode(2)).isEqualTo(RecordType.INFIMUM),
                () -> assertThat(RecordType.fromCode(3)).isEqualTo(RecordType.SUPREMUM)
        );
    }


    @DisplayName("RecordType Code와 일치하는 RecordType을 찾는데 실패 시 에러를 반환한다.")
    @Test
    void throwsExceptionForInvalidCode() {
        int recordTypeCode = -1;

        assertThrows(IllegalArgumentException.class, () -> RecordType.fromCode(recordTypeCode));
    }
}
