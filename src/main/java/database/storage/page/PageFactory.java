package database.storage.page;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class PageFactory {

    public static Page deserialize(ByteBuffer buffer) {
        buffer.mark();
        PageType pageType = PageType.fromCode(buffer.get());
        buffer.reset();

        Function<ByteBuffer, Page> deserializer = pageType.getDeserializer();
        return deserializer.apply(buffer);
    }
}
