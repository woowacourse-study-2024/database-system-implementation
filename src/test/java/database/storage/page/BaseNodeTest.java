package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.page.fspheader.BaseNode;
import database.storage.page.fspheader.Pointer;
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

        BaseNode baseNode = new BaseNode((short) 0, first, last);

        ByteBuffer buffer = ByteBuffer.allocate(BaseNode.SIZE);

        // when
        baseNode.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getShort()).as("Length").isEqualTo((short) 0),
                () -> assertThat(buffer.getInt()).as("FirstPointer pageNumber").isEqualTo(0),
                () -> assertThat(buffer.getInt()).as("FirstPointer offset").isEqualTo(100),
                () -> assertThat(buffer.getInt()).as("LastPointer pageNumber").isEqualTo(0),
                () -> assertThat(buffer.getInt()).as("LastPointer offset").isEqualTo(600)
        );
    }

    @DisplayName("BaseNode 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Pointer first = new Pointer(0, 100);
        Pointer last = new Pointer(0, 600);

        BaseNode original = new BaseNode((short) 0, first, last);

        ByteBuffer buffer = ByteBuffer.allocate(BaseNode.SIZE);
        original.serialize(buffer);
        buffer.flip();

        // when
        BaseNode deserialized = BaseNode.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getLength()).as("Length").isEqualTo(original.getLength()),
                () -> assertPointersEqual("FirstPointer", first, deserialized.getFirst()),
                () -> assertPointersEqual("LastPointer", last, deserialized.getLast())
        );
    }

    private void assertPointersEqual(String pointerName, Pointer expected, Pointer actual) {
        assertAll(
                () -> assertThat(actual.getPageNumber()).as(pointerName + " pageNumber")
                        .isEqualTo(expected.getPageNumber()),
                () -> assertThat(actual.getOffset()).as(pointerName + " offset").isEqualTo(expected.getOffset())
        );
    }
}
