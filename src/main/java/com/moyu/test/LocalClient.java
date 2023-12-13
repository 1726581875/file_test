package com.moyu.test;

import com.moyu.test.terminal.TcpTerminal;

/**
 * @author xiaomingzhang
 * @date 2023/12/11
 */
public class LocalClient {

    //private static final String ipAddress = "159.75.134.161";
    private static final String ipAddress = "localhost";
    private static final int port = 8888;

    public static void main(String[] args) {
        TcpTerminal tcpTerminal = new TcpTerminal(ipAddress, port);
        tcpTerminal.start();
    }

}
