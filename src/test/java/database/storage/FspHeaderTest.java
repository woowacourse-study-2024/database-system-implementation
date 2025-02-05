package database.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.page.Page;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FspHeaderTest {

    @DisplayName("FspHeader 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        BaseNode freeFrag = new BaseNode(new Pointer(0, 200), new Pointer(0, 200));
        BaseNode fullFrag = new BaseNode(new Pointer(0, 400), new Pointer(0, 600));
        BaseNode free = new BaseNode(new Pointer(0, 300), new Pointer(0, 300));
        byte[] entries = new byte[FspHeader.ENTRIES_SIZE];
        entries[0] = 5;
        entries[1] = 3;

        FspHeader header = new FspHeader(1, 0, freeFrag, fullFrag, free, entries);

        int capacity = Page.PAGE_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        // when
        header.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getInt()).isEqualTo(1),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(200),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(200),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(400),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(600),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(300),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(300),
                () -> assertThat(buffer.get()).isEqualTo((byte) 5),
                () -> assertThat(buffer.get()).isEqualTo((byte) 3)
        );
    }

    @DisplayName("FspHeader 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        BaseNode freeFrag = new BaseNode(new Pointer(0, 200), new Pointer(0, 200));
        BaseNode fullFrag = new BaseNode(new Pointer(0, 400), new Pointer(0, 600));
        BaseNode free = new BaseNode(new Pointer(0, 300), new Pointer(0, 300));
        byte[] entries = new byte[FspHeader.ENTRIES_SIZE];
        entries[0] = 5;
        entries[1] = 3;

        FspHeader original = new FspHeader(1, 0, freeFrag, fullFrag, free, entries);

        int capacity = Page.PAGE_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        original.serialize(buffer);
        buffer.flip();

        // when
        FspHeader deserialized = FspHeader.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getSpaceId()).isEqualTo(original.getSpaceId()),
                () -> assertThat(deserialized.getFreeFrag().getFirst().getOffset()).isEqualTo(
                        original.getFreeFrag().getFirst().getOffset()),
                () -> assertThat(deserialized.getFreeFrag().getFirst().getPageNumber())
                        .isEqualTo(original.getFreeFrag().getFirst().getPageNumber()),

                () -> assertThat(deserialized.getFullFrag().getFirst().getOffset())
                        .isEqualTo(original.getFullFrag().getFirst().getOffset()),
                () -> assertThat(deserialized.getFullFrag().getFirst().getPageNumber())
                        .isEqualTo(original.getFullFrag().getFirst().getPageNumber()),

                () -> assertThat(deserialized.getFree().getFirst().getOffset())
                        .isEqualTo(original.getFree().getFirst().getOffset()),
                () -> assertThat(deserialized.getFree().getFirst().getPageNumber())
                        .isEqualTo(original.getFree().getFirst().getPageNumber()),

                () -> assertThat(deserialized.getEntries()[0]).isEqualTo((byte) 5),
                () -> assertThat(deserialized.getEntries()[1]).isEqualTo((byte) 3)
        );
    }
}
