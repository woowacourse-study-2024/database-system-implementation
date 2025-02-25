package database.storage.datadictionary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IndexTest {

    @DisplayName("Index 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        Index index = new Index("idx_members_name", IndexType.SECONDARY, 23);
        ByteBuffer buffer = ByteBuffer.allocate(index.getByteSize());

        // when
        index.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(ByteBufferUtils.readString(buffer)).as("name").isEqualTo(index.getName()),
                () -> assertThat(buffer.get()).as("typeCode").isEqualTo((byte) 1),
                () -> assertThat(buffer.getInt()).as("rootPageNumber").isEqualTo(index.getRootPageNumber())
        );
    }

    @DisplayName("Index 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Index original = new Index("idx_clustered", IndexType.CLUSTERED, 2);
        ByteBuffer buffer = ByteBuffer.allocate(original.getByteSize());

        original.serialize(buffer);
        buffer.flip();

        // when
        Index deserialized = Index.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getName()).isEqualTo("idx_clustered"),
                () -> assertThat(deserialized.getType()).isEqualTo(IndexType.CLUSTERED),
                () -> assertThat(deserialized.getRootPageNumber()).isEqualTo(2)
        );
    }

}
