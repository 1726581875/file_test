package com.moyu.test.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author xiaomingzhang
 * @date 2023/7/5
 */
public class TcpServer {

    public static void main(String[] args) {

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(8888);
            int threadNum = 0;
            while (true) {
                Socket socket = serverSocket.accept();
                System.out.println("接收到请求");
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

}
