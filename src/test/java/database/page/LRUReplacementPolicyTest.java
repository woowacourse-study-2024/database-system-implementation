package database.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LRUReplacementPolicyTest {

    private static final String testFileName = "testfile.db";

    LinkedList<PageId> list;
    private PageReplacementPolicy lruReplacementPolicy;

    @BeforeEach
    public void setUp() {
        list = new LinkedList<>();
        lruReplacementPolicy = new LRUReplacementPolicy(list);
    }

    @Test
    @DisplayName("LRU 리스트에 페이지를 추가한다")
    public void addPage() {
        lruReplacementPolicy.addPage(new PageId(testFileName, 1));
        lruReplacementPolicy.addPage(new PageId(testFileName, 2));
        lruReplacementPolicy.addPage(new PageId(testFileName, 3));

        List<Integer> pageNums = list.stream().map(PageId::pageNum).toList();
        assertThat(pageNums).containsExactly(3, 2, 1);
    }

    @Test
    @DisplayName("LRU 리스트에서 페이지를 가장 앞으로 이동한다")
    public void updatePage() {
        lruReplacementPolicy.addPage(new PageId(testFileName, 1));
        lruReplacementPolicy.addPage(new PageId(testFileName, 2));
        lruReplacementPolicy.addPage(new PageId(testFileName, 3));

        lruReplacementPolicy.updatePage(new PageId(testFileName, 2));

        List<Integer> pageNums = list.stream().map(PageId::pageNum).toList();
        assertThat(pageNums).containsExactly(2, 3, 1);
    }

    @Test
    @DisplayName("LRU 리스트에 제거를 수행한다")
    public void evictPage() {
        lruReplacementPolicy.addPage(new PageId(testFileName, 1));
        lruReplacementPolicy.addPage(new PageId(testFileName, 2));
        lruReplacementPolicy.addPage(new PageId(testFileName, 3));

        PageId evictId = lruReplacementPolicy.evictPage();
        List<Integer> pageNums = list.stream().map(PageId::pageNum).toList();
        assertAll(
                () -> assertThat(evictId.pageNum()).isEqualTo(1),
                () -> assertThat(pageNums).containsExactly(3, 2)
        );
    }
}
