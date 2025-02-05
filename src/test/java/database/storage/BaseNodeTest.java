package database.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BaseNodeTest {

    @DisplayName("BaseNode 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        Pointer first = new Pointer(0, 100);
        Pointer last = new Pointer(0, 600);

        BaseNode baseNode = new BaseNode(first, last);

        int capacity = BaseNode.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        // when
        baseNode.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(100),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(600)
        );
    }

    @DisplayName("BaseNode 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Pointer first = new Pointer(0, 100);
        Pointer last = new Pointer(0, 600);

        BaseNode original = new BaseNode(first, last);

        int capacity = BaseNode.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        original.serialize(buffer);
        buffer.flip();

        // when
        BaseNode deserialized = BaseNode.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getFirst().getPageNumber())
                        .isEqualTo(original.getFirst().getPageNumber()),
                () -> assertThat(deserialized.getFirst().getOffset())
                        .isEqualTo(original.getFirst().getOffset()),

                () -> assertThat(deserialized.getLast().getPageNumber())
                        .isEqualTo(original.getLast().getPageNumber()),
                () -> assertThat(deserialized.getLast().getOffset())
                        .isEqualTo(original.getLast().getOffset())
        );
    }
}
