package com.moyu.xmz;

import com.moyu.xmz.net.TcpServer;

/**
 * @author xiaomingzhang
 * @date 2023/12/11
 */
public class LocalServer {

    public static void main(String[] args) {
        TcpServer tcpServer = new TcpServer();
        tcpServer.start();
    }

}
