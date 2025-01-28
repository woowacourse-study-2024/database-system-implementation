package database;

public class Application {

    public static void main(String[] args) {
        ParameterHandler parameterHandler = new ParameterHandler(args);
        DatabaseServer server = new DatabaseServer(parameterHandler.getPort());
        server.start();
    }
}
