package database.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PagedFileTest {

    private final String testFileName = "test.dat";

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileAccessor fileAccessor;

    @BeforeEach
    public void setUp() throws IOException {
        file = new File(testFileName);
        file.createNewFile();
        randomAccessFile = new RandomAccessFile(file, "rw");
        fileAccessor = new FileAccessor(randomAccessFile, PagedFile.PAGE_SIZE, FileHeader.HEADER_SIZE);
    }

    @AfterEach
    public void clearFile() {
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    @DisplayName("페이지를 할당받는다.")
    void allocatePage() throws IOException {
        PagedFile pagedFile = new PagedFile(randomAccessFile);

        int pageNum = pagedFile.allocatePage();

        assertAll(
                () -> assertThat(pageNum).isGreaterThanOrEqualTo(0),
                () -> assertThat(file.length()).isEqualTo(FileHeader.HEADER_SIZE + PagedFile.PAGE_SIZE)
        );
    }

    @Test
    @DisplayName("기존에 사용되었던 페이지를 재사용한다.")
    void allocatePageWithReuse() throws IOException {
        PagedFile pagedFile = new PagedFile(randomAccessFile);

        int firstPageNum = pagedFile.allocatePage();
        int secondPageNum = pagedFile.allocatePage();
        pagedFile.deallocatePage(firstPageNum);
        int newPageNum = pagedFile.allocatePage();

        assertAll(
                () -> assertThat(newPageNum).isEqualTo(firstPageNum),
                () -> assertThat(file.length()).isEqualTo(FileHeader.HEADER_SIZE + PagedFile.PAGE_SIZE * 2)
        );
    }

    @Test
    @DisplayName("페이지를 쓴다.")
    void writePage() throws IOException {
        PagedFile pagedFile = new PagedFile(randomAccessFile);
        int allocatePage = pagedFile.allocatePage();
        byte[] data = new byte[PagedFile.PAGE_SIZE];
        data[0] = 0b1010;

        pagedFile.writePage(allocatePage, data);

        byte[] actual = new byte[PagedFile.PAGE_SIZE];
        fileAccessor.readPage(allocatePage, actual);

        assertThat(actual[0]).isEqualTo((byte) 0b1010);
    }

    @Test
    @DisplayName("페이지를 읽는다.")
    void readPage() throws IOException {
        PagedFile pagedFile = new PagedFile(randomAccessFile);
        int allocatePage = pagedFile.allocatePage();
        byte[] data = new byte[PagedFile.PAGE_SIZE];
        data[0] = 0b1010;
        fileAccessor.writePage(allocatePage, data);

        Page page = pagedFile.readPage(allocatePage);

        byte[] actual = page.getData();
        assertThat(actual[0]).isEqualTo((byte) 0b1010);
    }
}
