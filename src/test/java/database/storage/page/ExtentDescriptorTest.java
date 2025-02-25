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
        ExtentDescriptor descriptor = new ExtentDescriptor((short) 1, prev, next, state, pageState);

        ByteBuffer buffer = ByteBuffer.allocate(ExtentDescriptor.SIZE);

        // when
        descriptor.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.getShort()).as("ExtentNumber").isEqualTo((short) 1),
                () -> assertThat(buffer.getInt()).as("PrevPointer pageNumber").isEqualTo(0),
                () -> assertThat(buffer.getInt()).as("PrevPointer offset").isEqualTo(150),
                () -> assertThat(buffer.getInt()).as("NextPointer pageNumber").isEqualTo(0),
                () -> assertThat(buffer.getInt()).as("NextPointer offset").isEqualTo(250),
                () -> assertThat(buffer.get()).as("ExtentState code").isEqualTo((byte) 2),
                () -> assertThat(getBitSet(buffer).get(0)).as("Page0 state").isTrue()
        );
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
        ExtentDescriptor original = new ExtentDescriptor((short) 1, prev, next, state, pageState);

        ByteBuffer buffer = ByteBuffer.allocate(ExtentDescriptor.SIZE);
        original.serialize(buffer);
        buffer.flip();

        // when
        ExtentDescriptor deserialized = ExtentDescriptor.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getExtentNumber())
                        .as("ExtentNumber").isEqualTo(original.getExtentNumber()),
                () -> assertPointersEqual("PrevPointer", prev, deserialized.getPrev()),
                () -> assertPointersEqual("NextPointer", next, deserialized.getNext()),
                () -> assertThat(deserialized.getState()).as("ExtentState").isEqualTo(state),
                () -> assertThat(deserialized.getPageState().get(0)).as("Page0 state").isTrue(),
                () -> assertThat(deserialized.getPageState().get(1)).as("Page1 state").isFalse()
        );
    }

    @DisplayName("Free 상태에서 페이지 할당 시 FreeFrag 상태로 전환하고 새 페이지 인덱스를 반환한다.")
    @Test
    void testAllocatePageForFreeState() {
        // given
        Pointer newPointer = Pointer.createNew();
        BitSet pages = new BitSet(4);
        pages.set(0, 4);

        ExtentDescriptor descriptor = new ExtentDescriptor((short) 0, newPointer, newPointer, ExtentState.FREE, pages);

        // when
        int newPageIndex = descriptor.allocatePage();

        // then
        assertAll(
                () -> assertThat(descriptor.getState()).isEqualTo(ExtentState.FREE_FRAG),
                () -> assertThat(newPageIndex).isEqualTo(0)
        );
    }

    @DisplayName("마지막 남은 여유 페이지를 할당하면 FullFrag 상태로 전환하고 새 페이지 인덱스를 반환한다.")
    @Test
    void testAllocatePageForFullyAllocated() {
        // given
        Pointer newPointer = Pointer.createNew();
        BitSet pages = new BitSet(3);
        pages.set(0, 3);

        ExtentDescriptor descriptor = new ExtentDescriptor((short) 0, newPointer, newPointer, ExtentState.FREE, pages);
        descriptor.allocatePage();
        descriptor.allocatePage();

        // when
        int newPageIndex = descriptor.allocatePage();

        // then
        assertAll(
                () -> assertThat(descriptor.getState()).isEqualTo(ExtentState.FULL_FRAG),
                () -> assertThat(newPageIndex).isEqualTo(2)
        );
    }

    @DisplayName("FullFrag 상태에서 페이지를 해제하면 FreeFrag 상태로 전환한다.")
    @Test
    void testDeallocatePageForFullState() {
        // given
        Pointer newPointer = Pointer.createNew();
        BitSet pages = new BitSet(3);

        ExtentDescriptor descriptor = new ExtentDescriptor((short) 0, newPointer, newPointer, ExtentState.FULL_FRAG,
                pages);

        // when
        descriptor.deallocatePage(0);

        // then
        assertAll(
                () -> assertThat(descriptor.getState()).isEqualTo(ExtentState.FREE_FRAG)
        );
    }

    @DisplayName("마지막 남은 할당 페이지를 해제하면 Free 상태로 전환한다.")
    @Test
    void testDeallocatePageForFullyFree() {
        // given
        Pointer newPointer = Pointer.createNew();
        BitSet pages = new BitSet(64);
        pages.set(0, 63);

        ExtentDescriptor descriptor = new ExtentDescriptor((short) 0, newPointer, newPointer, ExtentState.FULL_FRAG,
                pages);

        // when
        descriptor.deallocatePage(63);

        // then
        assertAll(
                () -> assertThat(descriptor.getState()).isEqualTo(ExtentState.FREE)
        );
    }

    private BitSet getBitSet(ByteBuffer buffer) {
        byte[] bitmap = new byte[ExtentDescriptor.BITMAP_BYTE_SIZE];
        buffer.get(bitmap);
        return BitSet.valueOf(bitmap);
    }

    private void assertPointersEqual(String pointerName, Pointer expected, Pointer actual) {
        assertAll(
                () -> assertThat(actual.getPageNumber()).as(pointerName + " pageNumber")
                        .isEqualTo(expected.getPageNumber()),
                () -> assertThat(actual.getOffset()).as(pointerName + " offset").isEqualTo(expected.getOffset())
        );
    }
}
