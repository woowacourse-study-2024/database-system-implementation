package database.storage.datadictionary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ColumnTest {

    @DisplayName("Column 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        Column column = new Column("age", DataType.INT, false, (short) 11);

        ByteBuffer buffer = ByteBuffer.allocate(column.getByteSize());

        // when
        column.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(ByteBufferUtils.readString(buffer)).as("name").isEqualTo(column.getName()),
                () -> assertThat(buffer.get()).as("typeCode").isEqualTo((byte) column.getType().getCode()),
                () -> assertThat(buffer.get()).as("nullable").isEqualTo((byte) 0),
                () -> assertThat(buffer.getShort()).as("length").isEqualTo(column.getLength())
        );
    }

    @DisplayName("Column 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Column original = new Column("name", DataType.CHAR, false, (short) 5);

        ByteBuffer buffer = ByteBuffer.allocate(original.getByteSize());
        original.serialize(buffer);
        buffer.flip();

        // when
        Column deserialized = Column.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getName()).isEqualTo(original.getName()),
                () -> assertThat(deserialized.getType()).isEqualTo(original.getType()),
                () -> assertThat(deserialized.getLength()).isEqualTo(original.getLength()),
                () -> assertThat(deserialized.isNullable()).isEqualTo(original.isNullable()),
                () -> assertThat(deserialized.getByteSize()).isEqualTo(original.getByteSize())
        );
    }
}
