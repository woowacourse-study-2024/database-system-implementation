package database.storageEngine.page;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("페이지 팩터리 테스트")
class PageFactoryTest {

    private final int pageNumber = 1234;

    @DisplayName("데이터 페이지 생성에 성공한다.")
    @Test
    void createDataPage() {
        // given&when
        Page page = PageFactory.createDataPage(pageNumber);

        // then
        assertThat(page.getPageType()).isEqualTo(PageType.PAGE_TYPE_CLUSTERED);
        assertThat(page.getPageNumber()).isEqualTo(pageNumber);
    }

    @DisplayName("인덱스 페이지 생성에 성공한다.")
    @Test
    void createIndexPage() {
        // given&when
        Page page = PageFactory.createIndexPage(pageNumber);

        // then
        assertThat(page.getPageType()).isEqualTo(PageType.PAGE_TYPE_BTR_INDEX);
        assertThat(page.getPageNumber()).isEqualTo(pageNumber);
    }

    @DisplayName("언두 페이지 생성에 성공한다.")
    @Test
    void createUndoPage() {
        // given&when
        Page page = PageFactory.createUndoPage(pageNumber);

        // then
        assertThat(page.getPageType()).isEqualTo(PageType.PAGE_TYPE_UNDO);
        assertThat(page.getPageNumber()).isEqualTo(pageNumber);
    }
}
