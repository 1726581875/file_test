package com.moyu.test.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author xiaomingzhang
 * @date 2023/7/5
 */
public class TcpServer {

    private static int port = 8888;

    public static void main(String[] args) {
        printDatabaseMsg();
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            int threadNum = 0;
            while (true) {
                Socket socket = serverSocket.accept();
                TcpServerThread tcpServerThread = new TcpServerThread(socket);
                Thread thread = new Thread(tcpServerThread, "TcpServerThread-" + threadNum);
                thread.setDaemon(true);
                thread.start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void printDatabaseMsg() {
        System.out.println("+-------------------------------+");
        System.out.println("|       YanSQL1.0  (^_^)        |");
        System.out.println("+-------------------------------+");
        System.out.println("TCP服务启动中，端口为" + port + "。等待连接...");
    }


}
