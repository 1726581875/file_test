package com.moyu.test;

import com.moyu.test.net.TcpServer;

/**
 * @author xiaomingzhang
 * @date 2023/12/11
 */
public class LocalServer {

    public static void main(String[] args) {
        TcpServer tcpServer = new TcpServer();
        tcpServer.listen();
    }

}
