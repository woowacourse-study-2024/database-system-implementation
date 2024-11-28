package database.storageEngine.exception;

public class TableCreationException extends RuntimeException {

    public TableCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}
