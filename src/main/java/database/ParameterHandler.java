package database;

import java.util.HashMap;
import java.util.Map;

public class ParameterHandler {

    private static final Map<String, String> DEFAULT_OPTIONS = Map.of(
            "-port", "3306",
            "-u", "root"
    );

    private final Map<String, String> options;

    public ParameterHandler(String[] args) {
        options = new HashMap<>(DEFAULT_OPTIONS);

        for (int i = 0; i < args.length; i += 2) {
            String option = args[i];
            if (!DEFAULT_OPTIONS.containsKey(option)) {
                throw new IllegalArgumentException("Unknown option: " + option);
            }

            String optionValue = args[i + 1];
            options.put(option, optionValue);
        }
    }

    public int getPort() {
        return Integer.parseInt(options.get("-port"));
    }

    public String getOption(String option) {
        return options.get(option);
    }
}
