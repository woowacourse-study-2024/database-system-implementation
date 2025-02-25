package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.page.fspheader.Pointer;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PointerTest {

    @DisplayName("NodePointer 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        Pointer pointer = new Pointer(0, 150);

        int capacity = Pointer.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        // when
        pointer.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getInt()).as("pageNumber").isEqualTo(0),
                () -> assertThat(buffer.getInt()).as("offset").isEqualTo(150)
        );
    }

    @DisplayName("NodePointer 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Pointer original = new Pointer(0, 150);

        int capacity = Pointer.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        original.serialize(buffer);
        buffer.flip();

        // when
        Pointer deserialized = Pointer.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getPageNumber()).isEqualTo(original.getPageNumber()),
                () -> assertThat(deserialized.getOffset()).isEqualTo(original.getOffset())
        );
    }
}
