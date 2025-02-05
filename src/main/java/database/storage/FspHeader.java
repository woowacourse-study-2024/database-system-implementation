package database.storage;

import database.storage.page.Page;
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
public class FspHeader {

    public static final int ENTRIES_SIZE = Page.PAGE_SIZE - (4 + 4 + BaseNode.SIZE * 3);

    private final int spaceId;
    private int size;

    private BaseNode freeFrag;
    private BaseNode fullFrag;
    private BaseNode free;

    private byte[] entries;

    public FspHeader(int spaceId, int size, BaseNode freeFrag, BaseNode fullFrag, BaseNode free, byte[] entries) {
        this.spaceId = spaceId;
        this.size = size;
        this.freeFrag = freeFrag;
        this.fullFrag = fullFrag;
        this.free = free;
        this.entries = entries;
    }

    public static FspHeader deserialize(ByteBuffer buffer) {
        int spaceId = buffer.getInt();
        int size = buffer.getInt();

        BaseNode freeFragList = BaseNode.deserialize(buffer);
        BaseNode fullFragList = BaseNode.deserialize(buffer);
        BaseNode free = BaseNode.deserialize(buffer);

        byte[] entries = new byte[ENTRIES_SIZE];
        buffer.get(entries);

        return new FspHeader(spaceId, size, freeFragList, fullFragList, free, entries);
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(spaceId);
        buffer.putInt(size);

        freeFrag.serialize(buffer);
        fullFrag.serialize(buffer);
        free.serialize(buffer);

        buffer.put(entries);
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
