package database.storage.bufferpool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.bufferpool.io.ByteBufferPool;
import database.storage.bufferpool.io.FileManager;
import database.storage.bufferpool.strategy.LRUStrategy;
import database.storage.bufferpool.strategy.PageReplacementStrategy;
import database.storage.page.Data;
import database.storage.page.FileExtension;
import database.storage.page.Page;
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
    private final String tableName = "jazz";

    private FileManager fileManager;

    @BeforeEach
    void setUp() {
        long totalMemory = Page.SIZE * 3;
        ByteBufferPool byteBufferPool = new ByteBufferPool(totalMemory, Page.SIZE);
        fileManager = new FileManager(byteBufferPool, fileExtension);
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
    void testLoadPageFromDisk() {
        // given
        Page page = Data.createNew(3);
        fileManager.writePage(page, tableName);

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool pool = new BufferPool(strategy, fileManager);

        // when
        PageId pageId = new PageId(tableName, page.getPageNumber());

        // then
        assertAll(
                () -> assertThat(pool.containsPage(pageId)).isEqualTo(false),
                () -> assertThat(pool.getPage(pageId).getPageNumber()).isEqualTo(page.getPageNumber()),
                () -> assertThat(pool.containsPage(pageId)).isEqualTo(true)
        );
    }

    @DisplayName("LRU 전략을 사용하는 버퍼 풀은 페이지 추가 시 가장 오래 된 페이지가 제거되어야 한다.")
    @Test
    void testPutPageWithLruStrategy() {
        // given
        Page page1 = Data.createNew(10);
        Page page2 = Data.createNew(11);
        Page page3 = Data.createNew(12);

        fileManager.writePage(page1, tableName);
        fileManager.writePage(page2, tableName);
        fileManager.writePage(page3, tableName);

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool pool = new BufferPool(strategy, fileManager);

        // when
        Page addedPage = Data.createNew(13);
        PageId addedpageId = new PageId(tableName, addedPage.getPageNumber());
        pool.putPage(addedpageId, addedPage);

        PageId removedPageId = new PageId(tableName, page1.getPageNumber());

        // then
        assertAll(
                () -> assertThat(pool.containsPage(removedPageId)).isFalse(),
                () -> assertThat(pool.containsPage(addedpageId)).isTrue()
        );
    }

    @DisplayName("플러시된 페이지는 클린 상태여야한다.")
    @Test
    void testFlushPageMarksPageAsClean() {
        // given
        Data page = Data.createNew(10);
        PageId pageId = new PageId(tableName, page.getPageNumber());
        page.makeDirty();

        int capacity = 3;
        PageReplacementStrategy<PageId> strategy = new LRUStrategy<>(capacity);
        BufferPool pool = new BufferPool(strategy, fileManager);

        pool.putPage(pageId, page);

        // when
        pool.flushPage(pageId);

        // then
        Data flushedPage = (Data) pool.getPage(pageId);
        assertThat(flushedPage.isDirty()).isFalse();
    }
}
