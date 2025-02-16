package database.storage.page;

import database.storage.page.fspheader.BaseNode;
import database.storage.page.fspheader.ExtentDescriptor;
import database.storage.page.fspheader.ExtentState;
import database.storage.page.fspheader.Pointer;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

/**
 * {@code FspHeader}는 {@code FileSpace}의 0번째 페이지로, 메타데이터와 {@code ExtentDescriptor} 엔트리들을 관리하는 역할을 한다.
 *
 * <p><b>spaceId</b>: {@code FileSpace}를 식별하는 고유 ID.
 * <p><b>size</b>: {@code FileSpace} 내에서 가장 높은 유효 페이지 번호를 나타낸다.
 * <p><b>BaseNode</b>: {@code ExtentDescriptor} 엔트리들을 가리키는 포인터를 저장하며, 플래그에 따라 구분된다.
 *  <ul>
 *      <li><b>freeFrag:</b> 여유 공간이 있는 페이지가 존재하는 Extent를 나타낸다. 일부 페이지는 할당되었지만,
 *      나머지 페이지는 비어있는 상태. Extent가 가득 차면 freeFrag에서 fullFrag로 이동한다.</li>
 *      <li><b>fullFrag</b>: 모든 페이지가 할당되어 여유 공간이 없는 Extent를 나타낸다. Extent가 가득 차면
 *      freeFrag에서 fullFrag로 이동하고, 페이지가 해제되어 여유 공간이 생기면 fullFrag에서 freeFrag로 다시 이동한다.</li>
 *      <li><b>free</b>: 아직 전혀 사용되지 않아 전체 할당 가능한 Extent를 나타낸다.</li>
 *  </ul>
 *
 * <p><b>entries</b>: {@code ExtentDescriptor} 엔트리들. 포인터 오프셋을 통해 다른 {@code ExtentDescriptor}에 접근할 수 있다.
 */
public class FspHeader extends AbstractPage {

    public static final int ENTRIES_SIZE = Page.SIZE - (FileHeader.SIZE + 4 + 4 + BaseNode.SIZE * 3);

    private final int spaceId;
    private int size;

    private BaseNode freeFrag;
    private BaseNode fullFrag;
    private BaseNode free;

    private final byte[] entries;

    public FspHeader(
            FileHeader fileHeader,
            int spaceId,
            int size,
            BaseNode freeFrag,
            BaseNode fullFrag,
            BaseNode free,
            byte[] entries
    ) {
        this.fileHeader = fileHeader;
        this.spaceId = spaceId;
        this.size = size;
        this.freeFrag = freeFrag;
        this.fullFrag = fullFrag;
        this.free = free;
        this.entries = entries;
    }

    public static FspHeader deserialize(ByteBuffer buffer) {
        FileHeader fspHeader = FileHeader.deserialize(buffer);
        int spaceId = buffer.getInt();
        int size = buffer.getInt();

        BaseNode freeFragList = BaseNode.deserialize(buffer);
        BaseNode fullFragList = BaseNode.deserialize(buffer);
        BaseNode free = BaseNode.deserialize(buffer);

        byte[] entries = new byte[ENTRIES_SIZE];
        buffer.get(entries);

        return new FspHeader(fspHeader, spaceId, size, freeFragList, fullFragList, free, entries);
    }

    @Override
    protected void serializeBody(ByteBuffer buffer) {
        buffer.putInt(spaceId);
        buffer.putInt(size);

        freeFrag.serialize(buffer);
        fullFrag.serialize(buffer);
        free.serialize(buffer);

        buffer.put(entries);
    }

    public int allocatePage() {
        if (!freeFrag.isEmpty()) {
            return allocatePageFromList(freeFrag, this::moveExtentFromFreeFragToFullFrag);
        }

        return allocatePageFromList(free, this::moveExtentFromFreeToFreeFrag);
    }

    private int allocatePageFromList(BaseNode list, BiConsumer<ExtentDescriptor, Pointer> move) {
        Pointer currentPointer = list.getFirst();
        ExtentDescriptor currentDescriptor = getDescriptor(currentPointer);

        int pageIndex = currentDescriptor.allocatePage();
        size++;

        if (currentDescriptor.isFullyAllocated()) {
            currentDescriptor.changeState(ExtentState.FULL_FRAG);
        }
        if (currentDescriptor.isFree()) {
            currentDescriptor.changeState(ExtentState.FREE_FRAG);
        }

        move.accept(currentDescriptor, currentPointer);
        return getGlobalPageNumber(currentDescriptor, pageIndex);
    }

    public void deallocatePage(int globalPageNumber) {
        int extentNumber = getExtentNumber(globalPageNumber);
        int pageIndex = getPageIndex(globalPageNumber);

        int offset = extentNumber * ExtentDescriptor.SIZE;

        Pointer currentPointer = getPointerForExtent(extentNumber);
        ExtentDescriptor currentDescriptor = getDescriptor(currentPointer);

        currentDescriptor.deallocatePage(pageIndex);
        size--;

        if (currentDescriptor.isFullFrag() && !currentDescriptor.isFullyAllocated()) {
            currentDescriptor.changeState(ExtentState.FREE_FRAG);
            moveExtentFromFullFragToFreeFrag(currentDescriptor, currentPointer);
        }
        if (currentDescriptor.isFreeFrag() && currentDescriptor.isFree()) {
            currentDescriptor.changeState(ExtentState.FREE);
            moveExtentFromFreeFragToFree(currentDescriptor, currentPointer);
        }

        ByteBuffer outBuffer = ByteBuffer.wrap(entries);
        outBuffer.position(offset);
        currentDescriptor.serialize(outBuffer);
    }

    private void moveExtentFromFreeFragToFullFrag(ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        freeFrag = removePointer(freeFrag, currentDescriptor, currentPointer);
        fullFrag = addPointer(fullFrag, currentDescriptor, currentPointer);
    }

    private void moveExtentFromFreeToFreeFrag(ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        free = removePointer(free, currentDescriptor, currentPointer);
        freeFrag = addPointer(freeFrag, currentDescriptor, currentPointer);
    }

    private void moveExtentFromFullFragToFreeFrag(ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        fullFrag = removePointer(fullFrag, currentDescriptor, currentPointer);
        freeFrag = addPointer(freeFrag, currentDescriptor, currentPointer);
    }

    private void moveExtentFromFreeFragToFree(ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        freeFrag = removePointer(freeFrag, currentDescriptor, currentPointer);
        free = addPointer(free, currentDescriptor, currentPointer);
    }

    private Pointer getPointerForExtent(int extentNumber) {
        int offset = extentNumber * ExtentDescriptor.SIZE;
        return new Pointer(0, offset);
    }

    private BaseNode removePointer(BaseNode list, ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        if (list.isEmpty()) {
            return list;
        }

        currentDescriptor.changePrev(Pointer.createNew());
        currentDescriptor.changeNext(Pointer.createNew());

        if (list.getFirst().equals(currentPointer)) {
            if (list.getLast().equals(currentPointer)) {
                return BaseNode.createNew();
            }

            return new BaseNode(currentDescriptor.getNext(), list.getLast());
        }

        if (list.getLast().equals(currentPointer)) {
            return new BaseNode(list.getFirst(), currentDescriptor.getPrev());
        }

        return list;
    }

    private BaseNode addPointer(BaseNode list, ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        if (list.isEmpty()) {
            return new BaseNode(currentPointer, currentPointer);
        }

        currentDescriptor.changePrev(list.getLast());
        return new BaseNode(list.getFirst(), currentPointer);
    }

    public ExtentDescriptor getDescriptor(Pointer pointer) {
        ByteBuffer buffer = ByteBuffer.wrap(entries);
        buffer.position(pointer.getOffset());
        return ExtentDescriptor.deserialize(buffer);
    }

    private int getGlobalPageNumber(ExtentDescriptor extent, int pageIndex) {
        return extent.getExtentNumber() * ExtentDescriptor.PAGES_PER_EXTENT + pageIndex;
    }

    private int getPageIndex(int globalPageNumber) {
        return globalPageNumber % ExtentDescriptor.PAGES_PER_EXTENT;
    }

    private int getExtentNumber(int globalPageNumber) {
        return globalPageNumber / ExtentDescriptor.PAGES_PER_EXTENT;
    }

    public int getSpaceId() {
        return spaceId;
    }

    public int getSize() {
        return size;
    }

    public BaseNode getFreeFrag() {
        return freeFrag;
    }

    public BaseNode getFullFrag() {
        return fullFrag;
    }

    public BaseNode getFree() {
        return free;
    }

    public byte[] getEntries() {
        return entries;
    }
}
