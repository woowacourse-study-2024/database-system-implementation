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

class FileManagerTest {

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

    @DisplayName("페이지를 저장한다.")
    @Test
    void writePageTest() {
        Page page = Page.createIndex(1, -1, -1, (short) 0);

        fileManager.writePage(page, tableName);

        Path filePath = Paths.get(FileManager.DIRECTORY_PATH, tableName + fileExtension.getExtension());
        assertThat(Files.exists(filePath)).isTrue();
    }

    @DisplayName("페이지를 읽어온다.")
    @Test
    void loadPageTest() {
        Page page = Page.createIndex(1, -1, -1, (short) 0);
        fileManager.writePage(page, tableName);

        Page loadedPage = fileManager.loadPage(tableName, page.getPageNumber());

        assertAll(
                () -> assertThat(loadedPage.getPageNumber()).isEqualTo(page.getPageNumber()),
                () -> assertThat(loadedPage.getFreeSpace()).isEqualTo(page.getFreeSpace())
        );
    }
}
