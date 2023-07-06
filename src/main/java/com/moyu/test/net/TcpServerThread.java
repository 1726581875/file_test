package com.moyu.test.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * @author xiaomingzhang
 * @date 2023/7/5
 */
public class TcpServerThread implements Runnable {

    private Socket socket;

    private DataInputStream in;

    private DataOutputStream out;

    public TcpServerThread(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
    }

    @Override
    public void run() {
        try {

            Integer databaseId = in.readInt();
            System.out.println(databaseId);
            Integer sqlCharLen = in.readInt();
            System.out.println(sqlCharLen);
            char[] sqlBytes = new char[sqlCharLen];
            for (int i = 0; i < sqlCharLen; i++) {
                sqlBytes[i] = in.readChar();
            }
            System.out.println("sql=" + new String(sqlBytes));


        } catch (Exception e) {
            try {
                in.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            try {
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

}
