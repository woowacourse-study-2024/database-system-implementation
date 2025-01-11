package database;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.engine.page.FileExtension;
import database.engine.page.Page;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BufferPoolTest {

    private final FileExtension fileExtension = FileExtension.IDB;

    private FileManager fileManager;

    private final String tableName = "jazz";

    @BeforeEach
    void setUp() {
        fileManager = new FileManager(fileExtension);
    }

    @AfterEach
    void tearDown() {
        Path path = Paths.get(FileManager.DIRECTORY_PATH);
        Path filePath = path.resolve(tableName + fileExtension.getExtension());

        try {
            if (Files.exists(filePath)) {
                Files.delete(filePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to clean up test files.", e);
        }
    }


    @DisplayName("페이지가 버퍼 풀에 존재하지 않으면 디스크에서 페이지를 불러온다.")
    @Test
    void loadPageFromDiskTest() {
        //given
        Page page = Page.createIndex(1, -1, -1, (short) 0);
        fileManager.writePage(page, tableName);

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool bufferPool = new BufferPool(strategy, capacity);

        //when
        PageId pageId = new PageId(tableName, page.getPageNumber());
        Page loadedPage = bufferPool.getPage(pageId);

        //then
        assertAll(
                () -> assertThat(loadedPage.getPageNumber()).isEqualTo(page.getPageNumber()),
                () -> assertThat(loadedPage.getPageHeader().isDirty()).isFalse(),
                () -> assertThat(loadedPage.getFreeSpace()).isEqualTo(page.getFreeSpace())
        );
    }

    @DisplayName("LRU 전략을 사용하는 버퍼 풀은 페이지 추가 시 가장 오래 된 페이지가 제거되어야 한다.")
    @Test
    void putPageWithLruStrategyTest() {
        //given
        Page page1 = Page.createIndex(1, -1, -1, (short) 0);
        Page page2 = Page.createIndex(2, -1, -1, (short) 0);
        Page page3 = Page.createIndex(3, -1, -1, (short) 0);

        fileManager.writePage(page1, tableName);
        fileManager.writePage(page2, tableName);
        fileManager.writePage(page3, tableName);

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool bufferPool = new BufferPool(strategy, capacity);

        //when
        Page addedPage = Page.createIndex(4, -1, -1, (short) 0);
        PageId addedpageId = new PageId(tableName, addedPage.getPageNumber());
        bufferPool.putPage(addedpageId, addedPage);

        PageId removedPageId = new PageId(tableName, page1.getPageNumber());

        //then
        assertAll(
                () -> assertThat(bufferPool.containsPage(removedPageId)).isFalse(),
                () -> assertThat(bufferPool.containsPage(addedpageId)).isTrue()
        );
    }

    @DisplayName("플러시된 페이지는 클린 상태여야한다.")
    @Test
    void flushPageMarksPageAsClean() {
        //given
        Page page = Page.createIndex(1, -1, -1, (short) 0);
        PageId pageId = new PageId(tableName, page.getPageNumber());
        page.makeDirty();

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool bufferPool = new BufferPool(strategy, capacity);

        bufferPool.putPage(pageId, page);

        //when
        bufferPool.flushPage(pageId);

        //then
        assertThat(bufferPool.getPage(pageId).isDirty()).isFalse();
    }

    @DisplayName("버퍼 풀에서 페이지를 제거한다.")
    @Test
    void removePageTest() {
        //given
        Page page = Page.createIndex(1, -1, -1, (short) 0);
        PageId pageId = new PageId(tableName, page.getPageNumber());

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool bufferPool = new BufferPool(strategy, capacity);

        bufferPool.putPage(pageId, page);

        //when
        bufferPool.removePage(pageId);

        //then
        assertThat(bufferPool.containsPage(pageId)).isFalse();
    }
}
