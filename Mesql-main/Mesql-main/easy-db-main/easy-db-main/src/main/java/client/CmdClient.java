package client;

import java.util.Scanner;


public class CmdClient{
    private Client client;

    public CmdClient(Client client) {
        this.client = client;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        String command;
        while (true) {
            System.out.print(">>> ");
            command = scanner.nextLine();
            String[] parts = command.split("\\s+");
            if (parts.length == 0) continue;

            String action = parts[0];
            switch (action.toLowerCase()) {
                case "exit":
                    return;
                case "set":
                    if (parts.length == 3) {
                        String key = parts[1];
                        String value = parts[2];
                        client.set(key, value);
                    } else {
                        System.out.println("指令格式错误，应是 set <key> <value>");
                    }
                    break;
                case "get":
                    if (parts.length == 2) {
                        String key = parts[1];
                        String value = client.get(key);
                        System.out.println("Value: " + value);
                    } else {
                        System.out.println("指令格式错误，应是 get <key>");
                    }
                    break;
                case "rm":
                    if (parts.length == 2) {
                        String key = parts[1];
                        client.rm(key);
                    } else {
                        System.out.println("指令格式错误，应是 rm <key>");
                    }
                    break;
                default:
                    System.out.println("指令格式错误，应是 set <key> <value>, get <key>, rm <key>, or exit");
            }
        }
    }

    public static void main(String[] args) {
        String host = "localhost";
        int port = 11111;
        Client client = new SocketClient(host, port);
        CmdClient cmdClient = new CmdClient(client);
        cmdClient.start();
    }
}
