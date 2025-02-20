package database.storage.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ByteBufferUtilsTest {

    @DisplayName("readString 사용 시 ByteBuffer를 사용해서 바이트 배열을 문자열로 읽어온다.")
    @Test
    void testReadString() {
        // given
        String original = "hello";
        byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);

        buffer.put((byte) original.length());
        buffer.put(bytes);
        buffer.flip();

        // when
        String result = ByteBufferUtils.readString(buffer);

        // then
        assertThat(result).isEqualTo(original);
    }

    @DisplayName("readString 사용 시 길이가 0이면 빈 문자열을 반환한다.")
    @Test
    void testReadStringEmptyWhenLengthZero() {
        // given
        String original = "";
        byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);

        buffer.put((byte) 0);
        buffer.put(bytes);
        buffer.flip();

        // when
        String result = ByteBufferUtils.readString(buffer);

        // when
        assertThat(result).isEqualTo(original);
    }

    @DisplayName("readString 사용 시 길이가 음수면 예외를 반환한다.")
    @Test
    void testReadStringThrowsExceptionWhenLengthNegative() {
        // given
        String original = "hi";
        byte[] bytes = original.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(1 + bytes.length);

        buffer.put((byte) -3);
        buffer.put(bytes);
        buffer.flip();

        // when & then
        assertThrows(IllegalArgumentException.class, () -> ByteBufferUtils.readString(buffer));
    }
}
