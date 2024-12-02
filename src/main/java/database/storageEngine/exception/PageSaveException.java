package database.storageEngine.exception;

public class PageSaveException extends RuntimeException {

    public PageSaveException(String message, Throwable cause) {
        super(message, cause);
    }
}
