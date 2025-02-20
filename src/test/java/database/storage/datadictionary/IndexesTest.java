package database.storage.datadictionary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class IndexesTest {

    @DisplayName("클러스터드 인덱스가 존재하지 않으면 예외를 반환한다.")
    @Test
    void throwsExceptionForMissingClusteredIndex() {
        Indexes indexes = new Indexes(List.of(
                new Index("idx_members_age", IndexType.SECONDARY, 1)
        ));

        assertThrows(NoSuchElementException.class, indexes::getClustered);
    }

    @DisplayName("클러스터드 인덱스를 반환한다.")
    @Test
    void returnsClusteredIndex() {
        Indexes indexes = new Indexes(List.of(
                new Index("idx_clustered", IndexType.CLUSTERED, 1),
                new Index("idx_members_age", IndexType.SECONDARY, 10)
        ));

        Index index = indexes.getClustered();

        assertAll(
                () -> assertThat(index.getName()).isEqualTo("idx_clustered"),
                () -> assertThat(index.getType()).isEqualTo(IndexType.CLUSTERED),
                () -> assertThat(index.getRootPageNumber()).isEqualTo(1)
        );
    }

    @DisplayName("인덱스 이름과 일치하는 인덱스가 존재하지 않으면 예외를 반환한다.")
    @Test
    void throwsExceptionForInvalidName() {
        Indexes indexes = new Indexes(List.of(
                new Index("idx_clustered", IndexType.CLUSTERED, 1),
                new Index("idx_members_age", IndexType.SECONDARY, 10)
        ));

        assertThrows(NoSuchElementException.class, () -> indexes.getIndex("idx_members_email"));
    }

    @DisplayName("인덱스 이름과 일치하는 인덱스를 반환한다.")
    @Test
    void returnsIndexForValidName() {
        Indexes indexes = new Indexes(List.of(
                new Index("idx_clustered", IndexType.CLUSTERED, 1),
                new Index("idx_members_age", IndexType.SECONDARY, 10)
        ));

        Index index = indexes.getIndex("idx_members_age");

        assertAll(
                () -> assertThat(index.getName()).isEqualTo("idx_members_age"),
                () -> assertThat(index.getType()).isEqualTo(IndexType.SECONDARY),
                () -> assertThat(index.getRootPageNumber()).isEqualTo(10)
        );
    }

}
