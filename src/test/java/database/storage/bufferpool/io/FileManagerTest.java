package database.storage.bufferpool.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import database.storage.page.Data;
import database.storage.page.FileExtension;
import database.storage.page.FileHeader;
import database.storage.page.Page;
import database.storage.page.PageHeader;
import database.storage.page.PageType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileManagerTest {

    private final FileExtension fileExtension = FileExtension.IDB;

    private FileManager fileManager;
    private final String tableName = "jazz";

    @BeforeEach
    void setUp() {
        ByteBufferPool pool = new ByteBufferPool(Page.SIZE * 3, Page.SIZE);
        fileManager = new FileManager(pool, fileExtension);
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

    @DisplayName("페이지를 저장한다.")
    @Test
    void writePageTest() {
        // given
        FileHeader fileHeader = new FileHeader(PageType.DATA, 1, -1, 2);
        PageHeader pageHeader = new PageHeader((short) 2, (short) 0, false);
        short freeSpace = Page.SIZE - (FileHeader.SIZE + PageHeader.SIZE + 2);
        byte[] recordData = new byte[freeSpace];

        Page page = new Data(fileHeader, pageHeader, freeSpace, recordData);

        // when
        fileManager.writePage(page, tableName);

        // then
        Path filePath = Paths.get(FileManager.DIRECTORY_PATH, tableName + fileExtension.getExtension());
        assertThat(Files.exists(filePath)).isTrue();
    }

    @DisplayName("페이지를 읽어온다.")
    @Test
    void loadPageTest() {
        // given
        FileHeader fileHeader = new FileHeader(PageType.DATA, 1, -1, 2);
        PageHeader pageHeader = new PageHeader((short) 2, (short) 0, false);
        short freeSpace = Page.SIZE - (FileHeader.SIZE + PageHeader.SIZE + 2);
        byte[] recordData = new byte[freeSpace];

        Page page = new Data(fileHeader, pageHeader, freeSpace, recordData);
        fileManager.writePage(page, tableName);

        // when
        Page loadedPage = fileManager.loadPage(tableName, page.getPageNumber());

        assertAll(
                () -> assertThat(loadedPage.getPageNumber()).isEqualTo(page.getPageNumber())
        );
    }
}
