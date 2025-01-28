package database.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.HashMap;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BufferTest {

    private static final int BUFFER_SIZE = 3;

    private HashMap<PageId, Page> pageBuffer;
    private Buffer buffer;


    @BeforeEach
    void setUp() {
        pageBuffer = new HashMap<>();
        buffer = new Buffer(BUFFER_SIZE, pageBuffer);
    }


    @Test
    @DisplayName("페이지를 버퍼에 추가한다.")
    void putAndGetPage() {
        PageId pageId = new PageId("file1", 1);
        Page page = new Page(1, "Data1".getBytes());

        buffer.putPage(pageId, page);

        assertThat(pageBuffer.get(pageId)).isEqualTo(page);
    }

    @Test
    @DisplayName("버퍼에 있는 페이지를 제거할 수 있다")
    void removePage() {
        PageId pageId = new PageId("file1", 1);
        Page page = new Page(1, "Data1".getBytes());
        pageBuffer.put(pageId, page);

        Page removePage = buffer.removePage(pageId);

        assertThat(removePage.getPageNum()).isEqualTo(1);
    }

    @Test
    @DisplayName("버퍼가 가득 차면 페이지를 추가할 수 없다")
    void putPageWhenBufferIsFull() {
        PageId pageId1 = new PageId("file1", 1);
        PageId pageId2 = new PageId("file2", 2);
        PageId pageId3 = new PageId("file3", 3);
        Page page1 = new Page(1, "Data1".getBytes());
        Page page2 = new Page(2, "Data2".getBytes());
        Page page3 = new Page(3, "Data3".getBytes());
        pageBuffer.put(pageId1, page1);
        pageBuffer.put(pageId2, page2);
        pageBuffer.put(pageId3, page3);

        PageId pageId4 = new PageId("file1", 4);
        Page page4 = new Page(4, "Data4".getBytes());

        assertThatThrownBy(() -> buffer.putPage(pageId4, page4))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Buffer is full");
    }

    @Test
    @DisplayName("버퍼에 페이지가 존재하는지 확인할 수 있다")
    void containsPage() {
        PageId pageId = new PageId("file1", 1);
        Page page = new Page(1, "Data1".getBytes());
        pageBuffer.put(pageId, page);

        assertThat(buffer.containsPage(pageId)).isTrue();
    }

    @Test
    @DisplayName("버퍼가 가득 찼는지 확인할 수 있다")
    void isFull() {
        PageId pageId1 = new PageId("file1", 1);
        PageId pageId2 = new PageId("file2", 2);
        PageId pageId3 = new PageId("file3", 3);
        Page page1 = new Page(1, "Data1".getBytes());
        Page page2 = new Page(2, "Data2".getBytes());
        Page page3 = new Page(3, "Data3".getBytes());
        pageBuffer.put(pageId1, page1);
        pageBuffer.put(pageId2, page2);
        pageBuffer.put(pageId3, page3);

        boolean actual = buffer.isFull();

        assertThat(actual).isTrue();
    }

    @Test
    @DisplayName("특정 파일에 대한 참조가 없는지 확인할 수 있다")
    void isNotReferenced() {
        PageId pageId = new PageId("file1", 1);
        Page page = new Page(1, "Data1".getBytes());
        pageBuffer.put(pageId, page);

        assertAll(
                () -> assertThat(buffer.isReferenced("file1")).isTrue(),
                () -> assertThat(buffer.isReferenced("file2")).isFalse()
        );
    }

    @Test
    @DisplayName("버퍼에 있는 모든 페이지 ID를 가져올 수 있다")
    void getPageIds() {
        PageId pageId1 = new PageId("file1", 1);
        PageId pageId2 = new PageId("file2", 2);
        PageId pageId3 = new PageId("file3", 3);
        Page page1 = new Page(1, "Data1".getBytes());
        Page page2 = new Page(2, "Data2".getBytes());
        Page page3 = new Page(3, "Data3".getBytes());
        pageBuffer.put(pageId1, page1);
        pageBuffer.put(pageId2, page2);
        pageBuffer.put(pageId3, page3);

        Set<PageId> actual = buffer.getPageIds();

        assertThat(actual).containsExactly(pageId1, pageId2, pageId3);
    }
}
