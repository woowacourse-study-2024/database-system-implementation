package database.storage.page;

import java.nio.ByteBuffer;

public interface Page {

    int SIZE = 16 * 1024;

    void serialize(ByteBuffer buffer);

    int getPageNumber();
}
