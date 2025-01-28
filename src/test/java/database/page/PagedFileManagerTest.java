package database.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PagedFileManagerTest {

    private final String testFileName = "test.dat";

    private HashMap<String, PagedFile> openFiles;
    private PagedFileManager pagedFileManager;

    @BeforeEach
    public void beforeEach() {
        openFiles = new HashMap<>();
        pagedFileManager = new PagedFileManager(openFiles);
    }

    @AfterEach
    public void afterEach() {
        File file = new File(testFileName);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    @DisplayName("페이지 파일을 생성한다.")
    void createFile() throws IOException {
        PagedFile pagedFile = pagedFileManager.createFile(testFileName);

        File actual = new File(testFileName);
        assertAll(
                () -> assertThat(actual).exists(),
                () -> assertThat(pagedFile).isNotNull()
        );
    }

    @Test
    @DisplayName("이미 존재하는 이름으로는 페이지 파일을 생성할 수 없다.")
    void createFileWhenDuplicateName() throws IOException {
        new File(testFileName).createNewFile();

        assertThatThrownBy(() -> pagedFileManager.createFile(testFileName))
                .isInstanceOf(IOException.class)
                .hasMessage("File already exists.");
    }

    @Test
    @DisplayName("페이지 파일을 삭제한다.")
    void deleteFile() throws IOException {
        pagedFileManager.createFile(testFileName);

        pagedFileManager.deleteFile(testFileName);

        assertThat(new File(testFileName)).doesNotExist();
    }

    @Test
    @DisplayName("삭제할 페이지 파일이 없으면 예외가 발생한다.")
    void deleteFileWhenNotFound() throws IOException {

        assertThatThrownBy(() -> pagedFileManager.deleteFile(testFileName))
                .isInstanceOf(IOException.class)
                .hasMessage("File not found.");
    }

    @Test
    @DisplayName("열려있는 페이지 파일은 삭제할 수 없다.")
    void deleteFileWhenFileIsOpen() throws IOException {
        pagedFileManager.createFile(testFileName);
        pagedFileManager.openFile(testFileName);

        assertThatThrownBy(() -> pagedFileManager.deleteFile(testFileName))
                .isInstanceOf(IOException.class)
                .hasMessage("File is currently open and cannot be deleted.");
    }

    @Test
    @DisplayName("페이지 파일을 연다.")
    void openAndCloseFile() throws IOException {
        pagedFileManager.createFile(testFileName);

        pagedFileManager.openFile(testFileName);

        assertThat(openFiles).containsKeys(testFileName);
    }

    @Test
    @DisplayName("이미 열려있는 페이지 파일을 반환한다.")
    void openFileAlreadyOpen() throws IOException {
        pagedFileManager.createFile(testFileName);
        PagedFile pagedFile = pagedFileManager.openFile(testFileName);

        PagedFile pagedFileAgain = pagedFileManager.openFile(testFileName);

        assertThat(pagedFileAgain).isEqualTo(pagedFile);
    }

    @Test
    @DisplayName("존재하지 않은 페이지 파일은 열 수 없다.")
    void openFileWhenNotFound() {

        assertThatThrownBy(() -> pagedFileManager.openFile(testFileName))
                .isInstanceOf(IOException.class)
                .hasMessage("File not found.");
    }

    @Test
    @DisplayName("페이지 파일을 닫는다.")
    void closeFile() throws IOException {
        pagedFileManager.createFile(testFileName);
        pagedFileManager.openFile(testFileName);

        pagedFileManager.closeFile(testFileName);

        assertThat(openFiles).doesNotContainKeys(testFileName);
    }

    @Test
    @DisplayName("열려있지 않은 페이지 파일은 닫을 수 없다.")
    void closeFileWhenNotOpen() {

        assertThatThrownBy(() -> pagedFileManager.closeFile(testFileName))
                .isInstanceOf(IOException.class)
                .hasMessage("File is not open.");
    }
}
