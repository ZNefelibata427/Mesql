package cli;

import client.SocketClient;

import java.util.Scanner;

public class CommandLineInterface {
    public static void main(String[] args) {
        SocketClient client = new SocketClient("localhost", 11111);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter command (set/get/rm/exit): ");
            String command = scanner.nextLine();
            if (command.equalsIgnoreCase("exit")) {
                break;
            }
            switch (command.toLowerCase()) {
                case "set":
                    System.out.print("Enter key: ");
                    String setKey = scanner.nextLine();
                    System.out.print("Enter value: ");
                    String setValue = scanner.nextLine();
                    client.set(setKey, setValue);
                    break;
                case "get":
                    System.out.print("Enter key: ");
                    String getKey = scanner.nextLine();
                    String value = client.get(getKey);
                    System.out.println("Value: " + value);
                    break;
                case "rm":
                    System.out.print("Enter key: ");
                    String rmKey = scanner.nextLine();
                    client.rm(rmKey);
                    break;
                default:
                    System.out.println("Invalid command");
                    break;
            }
        }
    }
}
