package database.storage.page;

import database.storage.page.fspheader.BaseNode;
import database.storage.page.fspheader.ExtentDescriptor;
import database.storage.page.fspheader.ExtentState;
import database.storage.page.fspheader.Pointer;
import java.nio.ByteBuffer;

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
    public static final int TOTAL_EXTENTS = 256;
    private static final int FIRST_EXTENT = 1;
    private static final int SECOND_EXTENT = 2;

    private final int spaceId;
    private int size;

    private final BaseNode freeFrag;
    private final BaseNode fullFrag;
    private final BaseNode free;

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

    public static FspHeader createNew(int spaceId) {
        FileHeader fileHeader = FileHeader.createNew(PageType.FSP_HEADER, 0);
        int size = 0;

        BaseNode freeFrag = createFreeFragNode();
        BaseNode fullFrag = createFullFragNode();
        BaseNode free = createFreeNode();

        byte[] entries = initializeEntries();

        return new FspHeader(fileHeader, spaceId, size, freeFrag, fullFrag, free, entries);
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
            return allocatePageFromFreeFrag();
        }

        return allocatePageFromFree();
    }

    public void deallocatePage(int globalPageNumber) {
        int extentNumber = getExtentNumber(globalPageNumber);
        int pageIndex = getPageIndex(globalPageNumber);

        Pointer pointer = getPointerForExtent(extentNumber);
        ExtentDescriptor descriptor = readDescriptor(pointer);

        if (descriptor.isFullFrag()) {
            deallocateFromFullFrag(descriptor, pageIndex, pointer);
        } else if (descriptor.isFreeFrag()) {
            deallocateFromFreeFrag(descriptor, pageIndex, pointer);
        }

        if (globalPageNumber == size) {
            size--;
        }
    }

    private static BaseNode createFreeFragNode() {
        return new BaseNode((short) 1, new Pointer(0, 0), new Pointer(0, 0));
    }

    private static BaseNode createFullFragNode() {
        return new BaseNode((short) 0, Pointer.createNew(), Pointer.createNew());
    }

    private static BaseNode createFreeNode() {
        int lastEntryOffset = getExtentDescriptorOffset(TOTAL_EXTENTS);
        return new BaseNode(
                (short) (TOTAL_EXTENTS - 1),
                new Pointer(0, ExtentDescriptor.SIZE),
                new Pointer(0, lastEntryOffset)
        );
    }

    private static byte[] initializeEntries() {
        byte[] entries = new byte[ENTRIES_SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(entries);

        writeFirstExtentDescriptor(buffer);
        writeRemainingExtentDescriptors(buffer);

        return entries;
    }

    private static void writeFirstExtentDescriptor(ByteBuffer buffer) {
        ExtentDescriptor firstDescriptor = ExtentDescriptor.createNew(FIRST_EXTENT, ExtentState.FREE_FRAG);
        firstDescriptor.allocatePage();
        firstDescriptor.serialize(buffer);
    }

    private static void writeRemainingExtentDescriptors(ByteBuffer buffer) {
        for (int extentNumber = SECOND_EXTENT; extentNumber <= TOTAL_EXTENTS; extentNumber++) {
            ExtentDescriptor descriptor = ExtentDescriptor.createNew(extentNumber, ExtentState.FREE);
            buffer.position(getExtentDescriptorOffset(extentNumber));
            descriptor.serialize(buffer);
        }
    }

    private int allocatePageFromFreeFrag() {
        Pointer pointer = freeFrag.getFirst();
        ExtentDescriptor descriptor = readDescriptor(pointer);

        int pageIndex = descriptor.allocatePage();
        size++;

        if (descriptor.isFullyAllocated()) {
            removeFirst(freeFrag, descriptor, pointer);
            addLast(fullFrag, descriptor, pointer);
        }
        writeDescriptor(descriptor, pointer);

        return getGlobalPageNumber(descriptor.getExtentNumber(), pageIndex);
    }

    private int allocatePageFromFree() {
        Pointer pointer = free.getFirst();
        ExtentDescriptor descriptor = readDescriptor(pointer);

        int pageIndex = descriptor.allocatePage();
        size++;

        removeFirst(free, descriptor, pointer);
        addLast(freeFrag, descriptor, pointer);
        writeDescriptor(descriptor, pointer);

        return getGlobalPageNumber(descriptor.getExtentNumber(), pageIndex);
    }

    private void deallocateFromFullFrag(ExtentDescriptor descriptor, int pageIndex, Pointer pointer) {
        descriptor.deallocatePage(pageIndex);
        removeFirst(fullFrag, descriptor, pointer);
        addLast(freeFrag, descriptor, pointer);
        writeDescriptor(descriptor, pointer);
    }

    private void deallocateFromFreeFrag(ExtentDescriptor descriptor, int pageIndex, Pointer pointer) {
        descriptor.deallocatePage(pageIndex);

        if (descriptor.isFullyFree()) {
            removeFirst(freeFrag, descriptor, pointer);
            addLast(free, descriptor, pointer);
        }
        writeDescriptor(descriptor, pointer);
    }

    private void removeFirst(BaseNode list, ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        if (list.isEmpty()) {
            return;
        }

        Pointer nextPointer = currentDescriptor.getNext();
        list.changeFirst(nextPointer);

        if (!nextPointer.isNull()) {
            ExtentDescriptor nextDescriptor = readDescriptor(currentDescriptor.getNext());
            nextDescriptor.changePrev(Pointer.createNew());
            writeDescriptor(nextDescriptor, nextPointer);
        }

        currentDescriptor.changePrev(Pointer.createNew());
        currentDescriptor.changeNext(Pointer.createNew());
        writeDescriptor(currentDescriptor, currentPointer);
        list.decreaseLength();
    }

    private void addLast(BaseNode list, ExtentDescriptor currentDescriptor, Pointer currentPointer) {
        Pointer lastPointer = list.getLast();

        if (!lastPointer.isNull()) {
            ExtentDescriptor lastDescriptor = readDescriptor(lastPointer);
            lastDescriptor.changeNext(currentPointer);
            writeDescriptor(lastDescriptor, lastPointer);
        }

        currentDescriptor.changePrev(lastPointer);
        list.changeLast(currentPointer);
        writeDescriptor(currentDescriptor, currentPointer);
        list.increaseLength();
    }

    private ExtentDescriptor readDescriptor(Pointer pointer) {
        ByteBuffer buffer = ByteBuffer.wrap(entries);
        buffer.position(pointer.getOffset());
        return ExtentDescriptor.deserialize(buffer);
    }

    private void writeDescriptor(ExtentDescriptor descriptor, Pointer pointer) {
        ByteBuffer buffer = ByteBuffer.wrap(entries);
        buffer.position(pointer.getOffset());
        descriptor.serialize(buffer);
    }


    private static int getExtentDescriptorOffset(int extentNumber) {
        return (extentNumber - 1) * ExtentDescriptor.SIZE;
    }

    private Pointer getPointerForExtent(int extentNumber) {
        int offset = getExtentDescriptorOffset(extentNumber);
        return new Pointer(0, offset);
    }

    private int getGlobalPageNumber(int extentNumber, int pageIndex) {
        return (extentNumber - 1) * ExtentDescriptor.PAGES_PER_EXTENT + pageIndex;
    }

    private int getPageIndex(int globalPageNumber) {
        return globalPageNumber % ExtentDescriptor.PAGES_PER_EXTENT;
    }

    private int getExtentNumber(int globalPageNumber) {
        return globalPageNumber / ExtentDescriptor.PAGES_PER_EXTENT + 1;
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
