package database;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ParameterHandlerTest {

    @Test
    @DisplayName("기본 포트는 3306이다.")
    void getPort() {
        String[] args = new String[]{};

        ParameterHandler parameterHandler = new ParameterHandler(args);

        assertThat(parameterHandler.getPort()).isEqualTo(3306);
    }

    @Test
    @DisplayName("포트를 지정할 수 있다.")
    void getPortWithOption() {
        String[] args = new String[]{"-port", "3307"};

        ParameterHandler parameterHandler = new ParameterHandler(args);

        assertThat(parameterHandler.getPort()).isEqualTo(3307);
    }

    @Test
    @DisplayName("옵션을 지정할 수 있다.")
    void getOption() {
        String[] args = new String[]{"-u", "user"};

        ParameterHandler parameterHandler = new ParameterHandler(args);

        assertThat(parameterHandler.getOption("-u")).isEqualTo("user");
    }
}
