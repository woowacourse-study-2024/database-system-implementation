package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.page.fspheader.ExtentDescriptor;
import database.storage.page.fspheader.ExtentState;
import database.storage.page.fspheader.Pointer;
import java.nio.ByteBuffer;
import java.util.BitSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExtentDescriptorTest {

    @DisplayName("ExtentDescriptor 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        Pointer prev = new Pointer(0, 150);
        Pointer next = new Pointer(0, 250);
        ExtentState state = ExtentState.FREE;
        BitSet pageState = new BitSet(ExtentDescriptor.PAGES_PER_EXTENT);
        pageState.set(0);
        ExtentDescriptor descriptor = new ExtentDescriptor((byte) 1, prev, next, state, pageState);

        int capacity = ExtentDescriptor.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        // when
        descriptor.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.get()).isEqualTo((byte) 1),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(150),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(250),
                () -> assertThat(buffer.get()).isEqualTo((byte) 2)
        );

        byte[] bitmap = new byte[ExtentDescriptor.BITMAP_BYTE_SIZE];
        buffer.get(bitmap);
        BitSet resultPageState = BitSet.valueOf(bitmap);
        assertThat(resultPageState.get(0)).isTrue();
    }

    @DisplayName("ExtentDescriptor 역직렬화 테스트")
    @Test
    void testDeserialize() {
        // given
        Pointer prev = new Pointer(0, 150);
        Pointer next = new Pointer(0, 250);
        ExtentState state = ExtentState.FREE;
        BitSet pageState = new BitSet(ExtentDescriptor.PAGES_PER_EXTENT);
        pageState.set(0);
        ExtentDescriptor original = new ExtentDescriptor((byte) 1, prev, next, state, pageState);

        int capacity = ExtentDescriptor.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        original.serialize(buffer);
        buffer.flip();

        // when
        ExtentDescriptor deserialized = ExtentDescriptor.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getExtentNumber()).isEqualTo(original.getExtentNumber()),
                () -> assertThat(deserialized.getNext().getPageNumber()).isEqualTo(original.getNext().getPageNumber()),
                () -> assertThat(deserialized.getNext().getOffset()).isEqualTo(original.getNext().getOffset()),
                () -> assertThat(deserialized.getPrev().getPageNumber()).isEqualTo(original.getPrev().getPageNumber()),
                () -> assertThat(deserialized.getPrev().getOffset()).isEqualTo(original.getPrev().getOffset()),
                () -> assertThat(deserialized.getState().getCode()).isEqualTo(original.getState().getCode()),
                () -> assertThat(deserialized.getPageState().get(0)).isTrue(),
                () -> assertThat(deserialized.getPageState().get(1)).isFalse()
        );
    }
}
