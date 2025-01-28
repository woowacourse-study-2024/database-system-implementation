package database.page;

import java.util.Map;
import java.util.Set;

public class Buffer {

    private final int bufferSize;
    private final Map<PageId, Page> pageBuffer;

    public Buffer(int bufferSize, Map<PageId, Page> pageBuffer) {
        this.bufferSize = bufferSize;
        this.pageBuffer = pageBuffer;
    }

    public Page getPage(PageId pageId) {
        if (!pageBuffer.containsKey(pageId)) {
            return null;
        }
        return pageBuffer.get(pageId);
    }

    public void putPage(PageId pageId, Page page) {
        if (pageBuffer.size() >= bufferSize) {
            throw new IllegalStateException("Buffer is full");
        }
        pageBuffer.put(pageId, page);
    }

    public Page removePage(PageId pageId) {
        if (!pageBuffer.containsKey(pageId)) {
            throw new IllegalStateException("Page not found in buffer");
        }
        return pageBuffer.remove(pageId);
    }

    public boolean containsPage(PageId pageId) {
        return pageBuffer.containsKey(pageId);
    }

    public boolean isFull() {
        return pageBuffer.size() >= bufferSize;
    }

    public boolean isReferenced(String filename) {
        return pageBuffer.keySet().stream()
                .map(PageId::filename)
                .anyMatch(filename::equals);
    }

    public Set<PageId> getPageIds() {
        return pageBuffer.keySet();
    }
}
