package database.server;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()), true)
        ) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                String response = handleRequest(inputLine);
                out.println(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String handleRequest(String request) {
        return "OK";
    }
}
