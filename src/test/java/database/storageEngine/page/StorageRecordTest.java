package database.storageEngine.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("레코드 테스트")
class StorageRecordTest {

    @DisplayName("레코드 생성에 성공한다.")
    @Test
    void createRecord() {
        // given
        List<Object> data = List.of("Chocochip", 123);
        StorageRecord storageRecord = new StorageRecord(data);

        // when
        List<Object> retrievedData = storageRecord.getValues();

        // then
        assertThat(retrievedData.size()).isEqualTo(data.size());
    }
}
