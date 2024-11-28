package database.storageEngine.page;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PageDirectory implements Serializable {

    private final List<Integer> recordPositions;

    public PageDirectory() {
        this.recordPositions = new ArrayList<>();
    }

    public void addRecordPosition(int position) {
        recordPositions.add(position);
    }

    public int getSize() {
        return recordPositions.size() * Integer.BYTES;
    }

    @Override
    public String toString() {
        return "PageDirectory{" +
                "recordPositions=" + recordPositions +
                '}';
    }
}
