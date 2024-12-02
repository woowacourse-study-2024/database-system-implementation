package database.engine;

import java.util.List;

public interface Handler {

    void insert(String tableName, Record record);

    List<Record> search(String tableName, Object key);

    void update(String tableName, Object key, byte[] newRecord);

    void delete(String tableName, Object key);
}

