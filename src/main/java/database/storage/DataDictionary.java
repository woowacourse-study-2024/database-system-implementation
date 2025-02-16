package database.storage;

import database.storage.datadictionary.Metadata;
import database.storage.strategy.PageReplacementStrategy;
import java.util.HashMap;
import java.util.Map;

public class DataDictionary {

    private static final String SYSTEM_TABLE_NAME = "information";

    private final Map<String, Metadata> tables;
    private final PageReplacementStrategy<String> strategy;

    public DataDictionary(PageReplacementStrategy<String> strategy) {
        this.tables = new HashMap<>();
        this.strategy = strategy;
    }

    public Metadata getMetadata(String tableName) {
        strategy.access(tableName);
        return tables.get(tableName);
    }
}
