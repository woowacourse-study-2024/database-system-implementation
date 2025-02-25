package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.page.fspheader.BaseNode;
import database.storage.page.fspheader.ExtentDescriptor;
import database.storage.page.fspheader.ExtentState;
import database.storage.page.fspheader.Pointer;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FspHeaderTest {

    @DisplayName("FspHeader 직렬화 테스트")
    @Test
    void testSerialize() {
        // given
        FileHeader fileHeader = new FileHeader(PageType.FSP_HEADER, 0, -1, 1);
        BaseNode freeFrag = new BaseNode((short) 0, new Pointer(0, 200), new Pointer(0, 200));
        BaseNode fullFrag = new BaseNode((short) 0, new Pointer(0, 400), new Pointer(0, 600));
        BaseNode free = new BaseNode((short) 0, new Pointer(0, 300), new Pointer(0, 300));
        byte[] entries = new byte[FspHeader.ENTRIES_SIZE];
        entries[0] = 5;
        entries[1] = 3;

        FspHeader header = new FspHeader(fileHeader, 1, 0, freeFrag, fullFrag, free, entries);

        int capacity = Page.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        // when
        header.serialize(buffer);
        buffer.flip();

        // then
        assertAll(
                () -> assertThat(buffer.get()).isEqualTo((byte) 1),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(-1),
                () -> assertThat(buffer.getInt()).isEqualTo(1),

                () -> assertThat(buffer.getInt()).isEqualTo(1),
                () -> assertThat(buffer.getInt()).isEqualTo(0),

                () -> assertThat(buffer.getShort()).isEqualTo((short) 0),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(200),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(200),

                () -> assertThat(buffer.getShort()).isEqualTo((short) 0),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(400),
                () -> assertThat(buffer.getInt()).isEqualTo(0),
                () -> assertThat(buffer.getInt()).isEqualTo(600),

                () -> assertThat(buffer.getShort()).isEqualTo((short) 0),
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
        FileHeader fileHeader = new FileHeader(PageType.FSP_HEADER, 0, -1, 1);
        BaseNode freeFrag = new BaseNode((short) 0, new Pointer(0, 200), new Pointer(0, 200));
        BaseNode fullFrag = new BaseNode((short) 0, new Pointer(0, 400), new Pointer(0, 600));
        BaseNode free = new BaseNode((short) 0, new Pointer(0, 300), new Pointer(0, 300));
        byte[] entries = new byte[FspHeader.ENTRIES_SIZE];
        entries[0] = 5;
        entries[1] = 3;

        FspHeader original = new FspHeader(fileHeader, 1, 0, freeFrag, fullFrag, free, entries);

        int capacity = Page.SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        original.serialize(buffer);
        buffer.flip();

        // when
        FspHeader deserialized = FspHeader.deserialize(buffer);

        // then
        assertAll(
                () -> assertThat(deserialized.getFileHeader().getPageType())
                        .isEqualTo(original.getFileHeader().getPageType()),
                () -> assertThat(deserialized.getFileHeader().getPageNumber())
                        .isEqualTo(original.getFileHeader().getPageNumber()),
                () -> assertThat(deserialized.getFileHeader().getPrevPageNumber())
                        .isEqualTo(original.getFileHeader().getPrevPageNumber()),
                () -> assertThat(deserialized.getFileHeader().getNextPageNumber())
                        .isEqualTo(original.getFileHeader().getNextPageNumber()),

                () -> assertThat(deserialized.getSpaceId()).isEqualTo(original.getSpaceId()),
                () -> assertThat(deserialized.getSize()).isEqualTo(original.getSize()),

                () -> assertThat(deserialized.getFreeFrag().getFirst().getOffset())
                        .isEqualTo(original.getFreeFrag().getFirst().getOffset()),
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

    @DisplayName("FspHeader createNew(초기화) 테스트")
    @Test
    void testCreateNew() {
        FspHeader fspHeader = FspHeader.createNew(1);

        BaseNode fullFrag = fspHeader.getFullFrag();
        BaseNode freeFrag = fspHeader.getFreeFrag();
        BaseNode free = fspHeader.getFree();

        // 파일 스페이스 헤더 생성시 FspHeader 0번 페이지가 첫 번째 Extent에 할당되면서 FreeFrag 리스트로 이동
        // fullFrag: empty
        // freeFrag: [xdes1]
        // free: [xdes2] <-> [xdes3] <->...<-> [xdes256]
        assertAll(
                () -> assertThat(fullFrag.isEmpty()).isTrue(),
                () -> assertThat(freeFrag.isEmpty()).isFalse(),
                () -> assertThat(free.isEmpty()).isFalse()
        );

        // ExtentDescriptor 엔트리 영역에서 주소상 첫 번째 Extent가 업데이트 되었는지 테스트
        // 페이지 할당 후 주소상 첫 번째 Extent는 FREE_FRAG로 업데이트, prev와 next 포인터도 빈 포인터를 할당
        ByteBuffer buffer = ByteBuffer.wrap(fspHeader.getEntries());
        ExtentDescriptor first = getExtentDescriptor(buffer, 0);
        assertAll(
                () -> assertThat(first.getExtentNumber()).isEqualTo((short) 1),
                () -> assertThat(first.getState()).isEqualTo(ExtentState.FREE_FRAG),
                () -> assertThat(first.getPageState().cardinality()).isEqualTo(ExtentDescriptor.PAGES_PER_EXTENT - 1),
                () -> assertThat(first.getPrev().isNull()).isTrue(),
                () -> assertThat(first.getNext().isNull()).isTrue()
        );

        // freeFrag 리스트가 업데이트 되었는지 테스트
        // freeFrag 리스트의 첫 번째 Extent는 주소상 첫 번째 Extent이어야 한다.
        ExtentDescriptor freeFragFirst = getExtentDescriptor(buffer, freeFrag.getFirst().getOffset());
        assertAll(
                () -> assertThat(freeFragFirst.getExtentNumber()).isEqualTo((short) 1),
                () -> assertThat(freeFragFirst.getState()).isEqualTo(ExtentState.FREE_FRAG),
                () -> assertThat(freeFragFirst.getPageState().cardinality())
                        .isEqualTo(ExtentDescriptor.PAGES_PER_EXTENT - 1),
                () -> assertThat(freeFragFirst.getPrev().isNull()).isTrue(),
                () -> assertThat(freeFragFirst.getNext().isNull()).isTrue()
        );

        // free 리스트가 업데이트 되었는지 테스트
        // free 리스트의 첫 번째 Extent는 주소상 두 번째 Extent이어야 한다.
        ExtentDescriptor second = getExtentDescriptor(buffer, ExtentDescriptor.SIZE);
        assertAll(
                () -> assertThat(second.getExtentNumber()).isEqualTo((short) 2),
                () -> assertThat(second.getState()).isEqualTo(ExtentState.FREE),
                () -> assertThat(second.getPrev().isNull()).isTrue(),
                () -> assertThat(second.getNext().isNull()).isFalse()
        );

        // free 리스트의 마지막 Extent
        ExtentDescriptor freeLast = getExtentDescriptor(buffer, free.getLast().getOffset());
        assertAll(
                () -> assertThat(freeLast.getExtentNumber()).isEqualTo((short) 256),
                () -> assertThat(freeLast.getState()).isEqualTo(ExtentState.FREE),
                () -> assertThat(freeLast.getPageState().cardinality())
                        .isEqualTo(ExtentDescriptor.PAGES_PER_EXTENT),
                () -> assertThat(freeLast.getPrev().isNull()).isFalse(),
                () -> assertThat(freeLast.getNext().isNull()).isTrue()
        );

        // FreeList의 중간 ExtentDescriptor
        for (int extentNumber = 3; extentNumber < FspHeader.TOTAL_EXTENTS; extentNumber++) {

            int currentOffset = (extentNumber - 1) * ExtentDescriptor.SIZE;
            ExtentDescriptor currentDescriptor = getExtentDescriptor(buffer, currentOffset);

            Pointer prevPointer = currentDescriptor.getPrev();
            Pointer nextPointer = currentDescriptor.getNext();

            ExtentDescriptor prevDescriptor = getExtentDescriptor(buffer, prevPointer.getOffset());
            Pointer prevNext = prevDescriptor.getNext();

            ExtentDescriptor nextDescriptor = getExtentDescriptor(buffer, nextPointer.getOffset());
            Pointer nextPrev = nextDescriptor.getPrev();

            assertAll(
                    () -> assertThat(prevPointer.isNull()).isFalse(),
                    () -> assertThat(prevNext.getOffset()).isEqualTo(currentOffset),
                    () -> assertThat(nextPointer.isNull()).isFalse(),
                    () -> assertThat(nextPrev.getOffset()).isEqualTo(currentOffset)
            );
        }
    }

    @DisplayName("allocatePage 시나리오 테스트")
    @Test
    void testAllocatePage() {
        // 파일 스페이스 헤더 생성
        FspHeader fspHeader = FspHeader.createNew(1);

        BaseNode fullFrag = fspHeader.getFullFrag();
        BaseNode freeFrag = fspHeader.getFreeFrag();
        BaseNode free = fspHeader.getFree();

        ByteBuffer buffer = ByteBuffer.wrap(fspHeader.getEntries());

        // 첫 번째 할당: freeFrag 리스트가 비어있지 않으므로 freeFrag 첫 번째 Extent에 페이지를 할당
        // freeFrag Extent에 아직 빈 페이지 슬롯이 있으므로 FullFrag 상태로 업데이트되지 않는다.
        int globalPageNumber = fspHeader.allocatePage();
        assertAll(
                () -> assertThat(globalPageNumber).isEqualTo(1),
                () -> assertThat(fspHeader.getSize()).isEqualTo(1),
                () -> assertThat(fspHeader.getFreeFrag().isEmpty()).isFalse(),
                () -> assertThat(fspHeader.getFullFrag().isEmpty()).isTrue(),
                () -> assertThat(fspHeader.getFree().isEmpty()).isFalse()
        );

        // freeFrag Extent에 나머지 페이지를 모두 할당
        for (int i = 2; i < ExtentDescriptor.PAGES_PER_EXTENT; i++) {
            fspHeader.allocatePage();
        }

        // 이제 주소상 첫 번째 Extent는 페이지가 모두 할당된 상태여야 한다.
        // freeFrag 리스트는 비어있게 되고, Extent는 fullFrag 리스트로 이동한다.
        assertAll(
                () -> assertThat(fullFrag.getLength()).isEqualTo((short) 1),
                () -> assertThat(freeFrag.getLength()).isEqualTo((short) 0),
                () -> assertThat(free.getLength()).isEqualTo((short) 255),
                () -> assertThat(freeFrag.getFirst().isNull()).isTrue(),
                () -> assertThat(fullFrag.getFirst().isNull()).isFalse()
        );

        // fullFrag 리스트의 Extent는 여유 페이지가 존재하지 않는다.
        ExtentDescriptor fullFragFirst = getExtentDescriptor(buffer, fullFrag.getFirst().getOffset());
        assertAll(
                () -> assertThat(fullFragFirst.getState()).isEqualTo(ExtentState.FULL_FRAG),
                () -> assertThat(fullFragFirst.getPageState().isEmpty()).isTrue()
        );

        // 추가로 새로운 페이지를 할당하면 free 리스트 Extent에서 페이지를 할당한다.
        fspHeader.allocatePage();
        assertAll(
                () -> assertThat(fullFrag.getLength()).isEqualTo((short) 1),
                () -> assertThat(freeFrag.getLength()).isEqualTo((short) 1),
                () -> assertThat(free.getLength()).isEqualTo((short) 254)
        );
    }

    @DisplayName("deallocatePage 시나리오 테스트")
    @Test
    void testDeallocatePage() {
        // 테스트를 위한 데이터 생성
        FspHeader fspHeader = FspHeader.createNew(2);
        for (int i = 1; i < ExtentDescriptor.PAGES_PER_EXTENT; i++) {
            fspHeader.allocatePage();
        }

        // 64번째 페이지 할당
        // fullFrag: [xdes1] (pageNum: 0~63) Extent 1은 모든 페이지를 할당하여 Full
        // freeFrag: [xdes2] (pageNum: 64~127) Extent 2은 페이지 1개를 할당하여 FreeFrag
        // free: [xdes3] <->...<-> [xdes256]
        int newPageNumber = fspHeader.allocatePage();

        BaseNode freeFrag = fspHeader.getFreeFrag();
        BaseNode fullFrag = fspHeader.getFullFrag();
        BaseNode free = fspHeader.getFree();

        assertAll(
                () -> assertThat(fullFrag.getLength()).isEqualTo((short) 1),
                () -> assertThat(freeFrag.getLength()).isEqualTo((short) 1),
                () -> assertThat(free.getLength()).isEqualTo((short) 254),
                () -> assertThat(newPageNumber).isEqualTo(64),
                () -> assertThat(freeFrag.isEmpty()).isFalse(),
                () -> assertThat(fullFrag.isEmpty()).isFalse(),
                () -> assertThat(free.isEmpty()).isFalse()
        );

        // 0번 페이지 해제: Extent 1은 상태가 FreeFrag로 업데이트되며 freeFrag 리스트 마지막으로 이동
        ByteBuffer buffer = ByteBuffer.wrap(fspHeader.getEntries());
        fspHeader.deallocatePage(0);

        assertAll(
                () -> assertThat(fullFrag.getLength()).isEqualTo((short) 0),
                () -> assertThat(freeFrag.getLength()).isEqualTo((short) 2),
                () -> assertThat(free.getLength()).isEqualTo((short) 254),
                () -> assertThat(freeFrag.isEmpty()).isFalse(),
                () -> assertThat(fullFrag.isEmpty()).isTrue(),
                () -> assertThat(free.isEmpty()).isFalse()
        );

        ExtentDescriptor freeFragLast = getExtentDescriptor(buffer, freeFrag.getLast().getOffset());
        ExtentDescriptor freeFragFirst = getExtentDescriptor(buffer, freeFrag.getFirst().getOffset());

        assertAll(
                () -> assertThat(freeFragLast.getState()).isEqualTo(ExtentState.FREE_FRAG),
                () -> assertThat(freeFragLast.getPageState().isEmpty()).isFalse(),
                () -> assertThat(freeFragLast.getPageState().cardinality()).isEqualTo(1),
                () -> assertThat(freeFragLast.getPrev().getOffset()).isEqualTo(freeFrag.getFirst().getOffset()),
                () -> assertThat(freeFragFirst.getNext().getOffset()).isEqualTo(freeFrag.getLast().getOffset()),
                () -> assertThat(freeFrag.getFirst().getOffset()).isEqualTo(ExtentDescriptor.SIZE),
                () -> assertThat(freeFrag.getLast().getOffset()).isEqualTo(0)
        );

        // 64번 페이지 해제: Extent 2는 상태가 Free로 업데이트되며 free 리스트 마지막으로 이동
        fspHeader.deallocatePage(64);

        assertAll(
                () -> assertThat(fullFrag.getLength()).isEqualTo((short) 0),
                () -> assertThat(freeFrag.getLength()).isEqualTo((short) 1),
                () -> assertThat(free.getLength()).isEqualTo((short) 255),
                () -> assertThat(freeFrag.getFirst().getOffset()).isEqualTo(0),
                () -> assertThat(freeFrag.getLast().getOffset()).isEqualTo(0),
                () -> assertThat(free.getLast().getOffset()).isEqualTo(ExtentDescriptor.SIZE)
        );
    }

    private ExtentDescriptor getExtentDescriptor(ByteBuffer buffer, int currentOffset) {
        buffer.position(currentOffset);
        return ExtentDescriptor.deserialize(buffer);
    }
}
