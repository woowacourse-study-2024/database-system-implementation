package database.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class PageTest {

    @Test
    @DisplayName("Page 를 생성한다")
    public void createPage() {
        Page page = new Page(1, "Initial Data".getBytes());

        assertAll(
                () -> assertThat(page.getPageNum()).isEqualTo(1),
                () -> assertThat(page.getData()).isEqualTo("Initial Data".getBytes()),
                () -> assertThat(page.isDirty()).isFalse()
        );
    }

    @Test
    @DisplayName("데이터를 변경하고, 페이지를 더티 상태로 만든다")
    public void updateData() {
        Page page = new Page(1, "Initial Data".getBytes());

        page.updateData("Updated Data".getBytes());

        assertAll(
                () -> assertThat(page.getData()).isEqualTo("Updated Data".getBytes()),
                () -> assertThat(page.isDirty()).isTrue()
        );
    }

    @Test
    @DisplayName("페이지를 깨끗한 상태로 만든다")
    public void flush() {
        Page page = new Page(1, "Initial Data".getBytes());
        page.updateData("New Data".getBytes());

        page.setClean();

        assertThat(page.isDirty()).isFalse();
    }
}
