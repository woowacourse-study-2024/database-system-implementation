package database.storage.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PageTypeTest {

    @DisplayName("PageTypeCode와 일치하는 PageType을 반환한다.")
    @Test
    void returnsPageTypeForValidCode() {
        int pageTypeCode = 0;

        PageType pageType = PageType.fromCode(pageTypeCode);

        assertThat(pageType).isEqualTo(PageType.INDEX);
    }


    @DisplayName("PageTypeCode와 일치하는 PageType을 찾는데 실패 시 에러를 반환한다.")
    @Test
    void throwsExceptionForInvalidCode() {
        int pageTypeCode = -1;

        assertThrows(IllegalArgumentException.class, () -> PageType.fromCode(pageTypeCode));
    }
}
