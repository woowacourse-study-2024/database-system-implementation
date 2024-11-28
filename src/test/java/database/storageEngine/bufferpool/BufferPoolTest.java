package database.storageEngine.bufferpool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storageEngine.bufferpool.pageReplacementStrategies.LRUStrategy;
import database.storageEngine.page.FileExtension;
import database.storageEngine.page.Page;
import database.storageEngine.page.PageFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("버퍼풀 테스트")
class BufferPoolTest {

    private static final String DIRECTORY_PATH = "disk";
    private static final FileExtension fileExtension = FileExtension.IDB;

    private final int capacity = 2;
    private final String tableName = "table";
    private BufferPool bufferPool;

    @BeforeEach
    void setUp() {
        PageReplacementStrategy<TablePageKey> lruStrategy = new LRUStrategy<>(capacity);
        bufferPool = new BufferPool(capacity, lruStrategy);
    }

    @DisplayName("버퍼풀 생성에 성공한다.")
    @Test
    void createBufferPool() {
        // given
        int pageNumber1 = 1;
        TablePageKey tablePageKey1 = new TablePageKey(tableName, pageNumber1);
        int pageNumber2 = 2;
        TablePageKey tablePageKey2 = new TablePageKey(tableName, pageNumber2);
        int pageNumber3 = 3;
        TablePageKey tablePageKey3 = new TablePageKey(tableName, pageNumber3);

        // when
        Page dataPage1 = PageFactory.createDataPage(pageNumber1);
        dataPage1.markDirty();
        bufferPool.putPage(tablePageKey1, dataPage1);
        bufferPool.flushPage(tablePageKey1);

        Page dataPage2 = PageFactory.createDataPage(pageNumber2);
        dataPage2.markDirty();
        bufferPool.putPage(tablePageKey2, dataPage2);
        bufferPool.flushPage(tablePageKey2);

        Page dataPage3 = PageFactory.createDataPage(pageNumber3);
        dataPage3.markDirty();
        bufferPool.putPage(tablePageKey3, dataPage3);
        bufferPool.flushPage(tablePageKey3);

        // then
        assertAll(
                () -> assertThat(bufferPool.getPage(tablePageKey1)).isPresent(),
                () -> assertThat(bufferPool.getPage(tablePageKey2)).isPresent(),
                () -> assertThat(bufferPool.getPage(tablePageKey3)).isPresent()
        );
    }

    @AfterEach
    void tearDown() {
        Path filePath = Paths.get(DIRECTORY_PATH, tableName + fileExtension.getExtension());
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            Files.deleteIfExists(Paths.get(DIRECTORY_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
