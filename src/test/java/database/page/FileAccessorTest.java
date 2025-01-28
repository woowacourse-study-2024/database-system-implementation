package database.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FileAccessorTest {

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
    @DisplayName("페이지에 데이터를 쓴다.")
    void writePage() throws IOException {
        int pageNum = 0;
        byte[] data = new byte[PagedFile.PAGE_SIZE];
        data[0] = 0b1010;

        fileAccessor.writePage(pageNum, data);

        byte[] actual = new byte[PagedFile.PAGE_SIZE];
        randomAccessFile.seek(FileHeader.HEADER_SIZE);
        randomAccessFile.readFully(actual);

        assertThat(actual[0]).isEqualTo((byte) 0b1010);
    }

    @Test
    @DisplayName("페이지에서 데이터를 읽는다.")
    void readPage() throws IOException {
        int pageNum = 0;
        byte[] data = new byte[PagedFile.PAGE_SIZE];
        data[0] = 0b1010;
        randomAccessFile.seek(FileHeader.HEADER_SIZE);
        randomAccessFile.write(data);

        byte[] actual = new byte[PagedFile.PAGE_SIZE];
        fileAccessor.readPage(pageNum, actual);

        assertThat(actual[0]).isEqualTo((byte) 0b1010);
    }

    @Test
    @DisplayName("페이지를 지운다.")
    void clearPage() throws IOException {
        int pageNum = 0;
        byte[] data = new byte[PagedFile.PAGE_SIZE];
        data[0] = 0b1010;
        randomAccessFile.seek(FileHeader.HEADER_SIZE);
        randomAccessFile.write(data);

        fileAccessor.clearPage(pageNum);

        byte[] actual = new byte[PagedFile.PAGE_SIZE];
        randomAccessFile.seek(FileHeader.HEADER_SIZE);
        randomAccessFile.readFully(actual);

        for (byte b : actual) {
            assertThat(b).isEqualTo((byte) 0);
        }
    }

    @Test
    @DisplayName("파일을 닫는다.")
    void closeFile() throws IOException {
        fileAccessor.close();

        assertThat(randomAccessFile.getChannel().isOpen()).isFalse();
    }
}
