package database.storage.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MetadataTest {

    @DisplayName("Metadata 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        Columns columns = new Columns(List.of(
                new Column("id", DataType.INT, false, (short) 11),
                new Column("name", DataType.CHAR, false, (short) 10)
        ));

        Metadata metadata = new Metadata(1, "members", 4, columns);
        ByteBuffer buffer = ByteBuffer.allocate(metadata.getByteSize());

        // when
        metadata.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getLong()).isEqualTo(metadata.getTableId()),
                () -> assertThat(ByteBufferUtils.readString(buffer)).isEqualTo(metadata.getTableName()),
                () -> assertThat(buffer.getInt()).isEqualTo(metadata.getColumnCount()),
                () -> assertThat(ByteBufferUtils.readString(buffer)).isEqualTo("id"),
                () -> assertThat(DataType.fromCode(buffer.get())).isEqualTo(DataType.INT),
                () -> assertThat(buffer.get()).isEqualTo((byte) 0),
                () -> assertThat(buffer.getShort()).isEqualTo((short) 11),
                () -> assertThat(ByteBufferUtils.readString(buffer)).isEqualTo("name"),
                () -> assertThat(DataType.fromCode(buffer.get())).isEqualTo(DataType.CHAR),
                () -> assertThat(buffer.get()).isEqualTo((byte) 0),
                () -> assertThat(buffer.getShort()).isEqualTo((short) 10)
        );
    }

    @DisplayName("Metadata 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Columns columns = new Columns(List.of(
                new Column("id", DataType.INT, false, (short) 11),
                new Column("name", DataType.CHAR, false, (short) 10)
        ));

        Metadata original = new Metadata(1, "members", 2, columns);
        ByteBuffer buffer = ByteBuffer.allocate(original.getByteSize());

        original.serialize(buffer);
        buffer.flip();

        // when
        Metadata deserialized = Metadata.deserialize(buffer);

        // then
        List<Column> deColumns = deserialized.getColumns().getColumns();
        Column first = deColumns.get(0);
        Column second = deColumns.get(1);

        assertAll(
                () -> assertThat(deserialized.getTableId()).isEqualTo(original.getTableId()),
                () -> assertThat(deserialized.getTableName()).isEqualTo(original.getTableName()),
                () -> assertThat(deserialized.getColumnCount()).isEqualTo(original.getColumnCount()),
                () -> assertThat(first.getName()).isEqualTo("id"),
                () -> assertThat(first.getType()).isEqualTo(DataType.INT),
                () -> assertThat(first.isNullable()).isEqualTo(false),
                () -> assertThat(first.getLength()).isEqualTo((short) 11),
                () -> assertThat(second.getName()).isEqualTo("name"),
                () -> assertThat(second.getType()).isEqualTo(DataType.CHAR),
                () -> assertThat(second.isNullable()).isEqualTo(false),
                () -> assertThat(second.getLength()).isEqualTo((short) 10)
        );
    }
}
