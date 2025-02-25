package database.storage.datadictionary;

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

        Indexes indexes = new Indexes(List.of(
                new Index("idx_clustered", IndexType.CLUSTERED, 6),
                new Index("idx_members_name", IndexType.SECONDARY, 23)
        ));

        Metadata metadata = new Metadata(1, "members", (byte) 2, columns, (byte) 2, indexes);
        ByteBuffer buffer = ByteBuffer.allocate(metadata.getByteSize());

        // when
        metadata.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getLong()).as("tableId").isEqualTo(metadata.getTableId()),
                () -> assertThat(ByteBufferUtils.readString(buffer)).as("tableName").isEqualTo(metadata.getTableName()),

                () -> assertThat(buffer.get()).as("columnCount").isEqualTo(metadata.getColumnCount()),

                () -> assertThat(ByteBufferUtils.readString(buffer)).as("column1 name").isEqualTo("id"),
                () -> assertThat(buffer.get()).as("column1 typeCode").isEqualTo((byte) 0),
                () -> assertThat(buffer.get()).as("column1 nullable").isEqualTo((byte) 0),
                () -> assertThat(buffer.getShort()).as("column1 length").isEqualTo((short) 11),

                () -> assertThat(ByteBufferUtils.readString(buffer)).as("column2 name").isEqualTo("name"),
                () -> assertThat(buffer.get()).as("column2 typeCode").isEqualTo((byte) 1),
                () -> assertThat(buffer.get()).as("column2 nullable").isEqualTo((byte) 0),
                () -> assertThat(buffer.getShort()).as("column2 length").isEqualTo((short) 10),

                () -> assertThat(buffer.get()).as("indexCount").isEqualTo(metadata.getIndexCount()),

                () -> assertThat(ByteBufferUtils.readString(buffer)).as("index1 name").isEqualTo("idx_clustered"),
                () -> assertThat(buffer.get()).as("index1 typeCode").isEqualTo((byte) 0),
                () -> assertThat(buffer.getInt()).as("index1 rootPageNumber").isEqualTo(6),

                () -> assertThat(ByteBufferUtils.readString(buffer)).as("index2 name").isEqualTo("idx_members_name"),
                () -> assertThat(buffer.get()).as("index2 typeCode").isEqualTo((byte) 1),
                () -> assertThat(buffer.getInt()).as("index2 rootPageNumber").isEqualTo(23)
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

        Indexes indexes = new Indexes(List.of(
                new Index("idx_clustered", IndexType.CLUSTERED, 6),
                new Index("idx_name", IndexType.SECONDARY, 23)
        ));

        Metadata original = new Metadata(1, "members", (byte) 2, columns, (byte) 2, indexes);
        ByteBuffer buffer = ByteBuffer.allocate(original.getByteSize());

        original.serialize(buffer);
        buffer.flip();

        // when
        Metadata deserialized = Metadata.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getTableId()).as("tableId").isEqualTo(original.getTableId()),
                () -> assertThat(deserialized.getTableName()).as("tableName").isEqualTo(original.getTableName()),

                () -> assertThat(deserialized.getColumnCount()).as("columnCount").isEqualTo(original.getColumnCount()),
                () -> assertColumnEquals(original.getColumn("id"), deserialized.getColumn("id")),
                () -> assertColumnEquals(original.getColumn("name"), deserialized.getColumn("name")),

                () -> assertThat(deserialized.getIndexCount()).as("indexCount").isEqualTo(original.getIndexCount()),
                () -> assertIndexEquals(original.getIndex("idx_clustered"), deserialized.getIndex("idx_clustered")),
                () -> assertIndexEquals(original.getIndex("idx_name"), deserialized.getIndex("idx_name"))
        );
    }

    private void assertColumnEquals(Column expected, Column actual) {
        assertAll(
                () -> assertThat(actual.getName()).as("name").isEqualTo(expected.getName()),
                () -> assertThat(actual.getType()).as("type").isEqualTo(expected.getType()),
                () -> assertThat(actual.isNullable()).as("nullable").isEqualTo(expected.isNullable()),
                () -> assertThat(actual.getLength()).as("length").isEqualTo(expected.getLength())
        );
    }

    private void assertIndexEquals(Index expected, Index actual) {
        assertAll(
                () -> assertThat(actual.getName()).as("name").isEqualTo(expected.getName()),
                () -> assertThat(actual.getType()).as("type").isEqualTo(expected.getType()),
                () -> assertThat(actual.getRootPageNumber())
                        .as("rootPageNumber")
                        .isEqualTo(expected.getRootPageNumber())
        );
    }
}
