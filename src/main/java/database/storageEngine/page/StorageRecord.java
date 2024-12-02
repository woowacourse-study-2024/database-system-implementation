package database.storageEngine.page;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

public class StorageRecord implements Serializable {

    private final List<Object> values;

    public StorageRecord(List<Object> values) {
        this.values = values;
    }

    public boolean contains(Object key) {
        return values.contains(key);
    }

    public List<Object> getValues() {
        return values;
    }

    public int getSize() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(values);
            oos.flush();
            oos.close();
            return baos.size();
        } catch (java.io.IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    public String toString() {
        return "StorageRecord{" +
                "values=" + values +
                '}';
    }
}
