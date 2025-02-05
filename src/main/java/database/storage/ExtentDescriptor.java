package database.storage;

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

    public static final int SIZE = 1 + 8 + 8 + 1 + 8;
    public static final int PAGES_PER_EXTENT = 64;
    public static final int BITMAP_BYTE_SIZE = 8;

    private final byte extentNumber;
    private final Pointer prev;
    private final Pointer next;
    private final ExtentState state;
    private final BitSet pageState;

    public ExtentDescriptor(
            byte extentNumber,
            Pointer prev,
            Pointer next,
            ExtentState state,
            BitSet pageState
    ) {
        this.extentNumber = extentNumber;
        this.prev = prev;
        this.next = next;
        this.state = state;
        this.pageState = pageState;
    }

    public static ExtentDescriptor deserialize(ByteBuffer buffer) {
        byte extentNumber = buffer.get();
        Pointer prev = Pointer.deserialize(buffer);
        Pointer next = Pointer.deserialize(buffer);
        ExtentState state = ExtentState.fromCode(buffer.get());
        BitSet pageState = deserializePageState(buffer);

        return new ExtentDescriptor(extentNumber, prev, next, state, pageState);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.put(extentNumber);
        prev.serialize(buffer);
        next.serialize(buffer);
        buffer.put((byte) state.getCode());
        serializePageState(buffer);
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

    public byte getExtentNumber() {
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
