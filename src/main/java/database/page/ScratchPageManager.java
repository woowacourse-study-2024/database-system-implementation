package database.page;

public class ScratchPageManager {

    private static final String SCRATCH_FILE = "scratch";

    private int nextPageNum = 0;

    public PageId allocateScratchPageId() {
        int pageNum = nextPageNum++;
        return new PageId(SCRATCH_FILE, pageNum);
    }

    public boolean isScratchPage(PageId pageId) {
        return SCRATCH_FILE.equals(pageId.filename());
    }
}
