package database.storage.io;

public enum FileExtension {

    IDB("idb"),
    FRM("frm"),
    ;

    FileExtension(String extension) {
        this.extension = extension;
    }

    private final String extension;

    public String getExtension() {
        return extension;
    }
}
