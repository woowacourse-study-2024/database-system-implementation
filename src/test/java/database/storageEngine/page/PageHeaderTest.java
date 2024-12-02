package database.storageEngine.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("페이지 헤더 테스트")
class PageHeaderTest {

    @DisplayName("페이지 헤더를 생성한다.")
    @Test
    void createPageHeader() {
        // given
        PageType pageType = PageType.PAGE_TYPE_CLUSTERED;
        int initialRecordCnt = 0;

        // when
        PageHeader pageHeader = new PageHeader(pageType);

        // then
        assertAll(
                () -> assertThat(pageHeader.getPageType()).isEqualTo(pageType),
                () -> assertThat(pageHeader.getRecordCount()).isEqualTo(initialRecordCnt),
                () -> assertThat(pageHeader.isDirty()).isFalse()
        );
    }

    @DisplayName("페이지 레코드 수가 증가한다.")
    @Test
    void incrementRecord() {
        // given
        PageType pageType = PageType.PAGE_TYPE_CLUSTERED;
        int recordCnt = 2;

        // when
        PageHeader pageHeader = new PageHeader(pageType);
        IntStream.range(0, recordCnt)
                .forEach(i -> pageHeader.incrementRecordCount());

        // then
        assertThat(pageHeader.getRecordCount()).isEqualTo(recordCnt);
    }

    @DisplayName("더티 페이지로 설정한다.")
    @Test
    void markDirty() {
        // given
        PageType pageType = PageType.PAGE_TYPE_CLUSTERED;
        PageHeader pageHeader = new PageHeader(pageType);

        // when
        pageHeader.markDirty();

        // then
        assertThat(pageHeader.isDirty()).isTrue();
    }
}
