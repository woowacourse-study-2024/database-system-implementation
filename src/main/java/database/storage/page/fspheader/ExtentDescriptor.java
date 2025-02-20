package database.storage.page.fspheader;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * {@code ExtentDescriptor}는 64개의 {@code Page} 묶음인 Extent를 관리하는 역할을 한다.
 * 각 Extent의 상태와 해당 Extent 내 개별 페이지들의 할당 정보를 포함한다.
 *
 * <p><b>extentNumber:</b> 해당 Extent 번호를 나타내며 1바이트 크기로 저장한다. (0 ~ 255)
 * <p><b>state:</b> extent 상태를 나타내며 다음과 같은 값 중 하나를 가질 수 있다.
 *  <ul>
 *      <li><b>FREE_FLAG:</b> 여유 공간이 있는 페이지가 존재하는 Extent를 나타낸다.</li>
 *      <li><b>FULL_FLAG</b>: 모든 페이지가 할당되어 여유 공간이 없는 Extent를 나타낸다.</li>
 *      <li><b>FREE</b>: 아직 전혀 사용되지 않아 전체 할당 가능한 Extent를 나타낸다.</li>
 *  </ul>
 *
 * <p><b>pageState:</b> 각 페이지의 상태를 나타내는 비트맵이다.
 * 비트 값이 1이면 페이지가 free(페이지가 할당되지 않아 비어있는 상태) 상태를, 0이면 used(이미 사용중인 상태) 상태를 의미한다.
 */
public class ExtentDescriptor {

    public static final int SIZE = 2 + 8 + 8 + 1 + 8;
    public static final int PAGES_PER_EXTENT = 64;
    public static final int BITMAP_BYTE_SIZE = 8;

    private static final int FIRST_EXTENT = 1;
    private static final int SECOND_EXTENT = 2;
    public static final int TOTAL_EXTENTS = 256;

    private final short extentNumber;
    private Pointer prev;
    private Pointer next;
    private ExtentState state;
    private final BitSet pageState;

    public ExtentDescriptor(short extentNumber, Pointer prev, Pointer next, ExtentState state, BitSet pageState) {
        this.extentNumber = extentNumber;
        this.prev = prev;
        this.next = next;
        this.state = state;
        this.pageState = pageState;
    }

    public static ExtentDescriptor createNew(int extentNumber, ExtentState state) {
        Pointer prev = getPrevPointer(extentNumber);
        Pointer next = getNextPointer(extentNumber);

        BitSet pageState = new BitSet(ExtentDescriptor.PAGES_PER_EXTENT);
        pageState.set(0, ExtentDescriptor.PAGES_PER_EXTENT);

        return new ExtentDescriptor((short) extentNumber, prev, next, state, pageState);
    }

    public static ExtentDescriptor deserialize(ByteBuffer buffer) {
        short extentNumber = buffer.getShort();
        Pointer prev = Pointer.deserialize(buffer);
        Pointer next = Pointer.deserialize(buffer);
        ExtentState state = ExtentState.fromCode(buffer.get());
        BitSet pageState = deserializePageState(buffer);

        return new ExtentDescriptor(extentNumber, prev, next, state, pageState);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putShort(extentNumber);
        prev.serialize(buffer);
        next.serialize(buffer);
        buffer.put((byte) state.getCode());
        serializePageState(buffer);
    }

    public int allocatePage() {
        int freePageNumber = pageState.nextSetBit(0);
        pageState.clear(freePageNumber);

        if (isFree()) {
            state = ExtentState.FREE_FRAG;
        }
        if (isFullyAllocated()) {
            state = ExtentState.FULL_FRAG;
        }

        return freePageNumber;
    }

    public void deallocatePage(int pageIndex) {
        pageState.set(pageIndex, true);

        if (isFullFrag()) {
            state = ExtentState.FREE_FRAG;
        }
        if (isFreeFrag() && isFullyFree()) {
            state = ExtentState.FREE;
        }
    }

    public boolean isFullyAllocated() {
        return pageState.isEmpty();
    }

    public boolean isFullyFree() {
        return pageState.cardinality() == PAGES_PER_EXTENT;
    }

    public boolean isFree() {
        return state == ExtentState.FREE;
    }

    public boolean isFreeFrag() {
        return state == ExtentState.FREE_FRAG;
    }

    public boolean isFullFrag() {
        return state == ExtentState.FULL_FRAG;
    }

    private static Pointer getPrevPointer(int extentNumber) {
        Pointer prev;
        if (extentNumber == FIRST_EXTENT || extentNumber == SECOND_EXTENT) {
            prev = Pointer.createNew();
        } else {
            prev = new Pointer(0, (extentNumber - 2) * ExtentDescriptor.SIZE);
        }
        return prev;
    }

    private static Pointer getNextPointer(int extentNumber) {
        Pointer next;
        if (extentNumber == FIRST_EXTENT || extentNumber == TOTAL_EXTENTS) {
            next = Pointer.createNew();
        } else {
            next = new Pointer(0, extentNumber * ExtentDescriptor.SIZE);
        }
        return next;
    }

    private static BitSet deserializePageState(ByteBuffer buffer) {
        byte[] bitmap = new byte[BITMAP_BYTE_SIZE];
        buffer.get(bitmap);
        return BitSet.valueOf(bitmap);
    }

    private void serializePageState(ByteBuffer buffer) {
        byte[] byteArray = pageState.toByteArray();
        byte[] bitmap = new byte[BITMAP_BYTE_SIZE];
        System.arraycopy(byteArray, 0, bitmap, 0, Math.min(byteArray.length, BITMAP_BYTE_SIZE));
        buffer.put(bitmap);
    }

    public void changePrev(Pointer pointer) {
        prev = pointer;
    }

    public void changeNext(Pointer pointer) {
        next = pointer;
    }

    public short getExtentNumber() {
        return extentNumber;
    }

    public Pointer getPrev() {
        return prev;
    }

    public Pointer getNext() {
        return next;
    }

    public ExtentState getState() {
        return state;
    }

    public BitSet getPageState() {
        return pageState;
    }
}
