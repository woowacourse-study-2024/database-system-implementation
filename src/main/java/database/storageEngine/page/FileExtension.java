package database.storageEngine.page;

public enum FileExtension {

    IDB(".idb"),
    ;

    private final String extension;

    FileExtension(String extension) {
        this.extension = extension;
    }

    public String getExtension() {
        return extension;
    }
}
