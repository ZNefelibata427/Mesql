
package example;

import client.Client;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 11111;
        Client client = new SocketClient(host, port);
        client.set("qer","12");
        client.set("qer","mesql");
    }
}