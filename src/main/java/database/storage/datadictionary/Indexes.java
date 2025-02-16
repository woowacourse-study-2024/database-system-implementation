package database.storage.datadictionary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class Indexes {

    private final List<Index> indexes;

    public Indexes(List<Index> indexes) {
        this.indexes = indexes;
    }

    public static Indexes deserialize(ByteBuffer buffer, byte indexCount) {
        List<Index> indexes = new ArrayList<>();

        for (byte i = 0; i < indexCount; i++) {
            indexes.add(Index.deserialize(buffer));
        }

        return new Indexes(indexes);
    }

    public Index getClustered() {
        return indexes.stream()
                .filter(Index::isClustered)
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Clustered index not found"));
    }

    public Index getIndex(String indexName) {
        return indexes.stream()
                .filter(index -> index.hasName(indexName))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Index not found: " + indexName));
    }

    public void serialize(ByteBuffer buffer) {
        indexes.forEach(index -> index.serialize(buffer));
    }

    public int getByteSize() {
        return indexes.stream()
                .mapToInt(Index::getByteSize)
                .sum();
    }

    public List<Index> getIndexes() {
        return indexes;
    }
}
