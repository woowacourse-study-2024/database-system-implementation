package database.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class DatabaseServer {
    private int port;
    private boolean isRunning;

    public DatabaseServer(int port) {
        this.port = port;
    }

    public void start() {
        isRunning = true;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Database server started on port " + port);
            while (isRunning) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        isRunning = false;
        System.out.println("Server stopped.");
    }
}
