package com.moyu.test.net;

import com.moyu.test.command.Command;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.SqlParser;
import com.moyu.test.net.packet.ErrPacket;
import com.moyu.test.net.packet.OkPacket;
import com.moyu.test.session.ConnectSession;

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

            // 读取数据库id
            Integer databaseId = in.readInt();
            // 读取语句长度
            Integer sqlCharLen = in.readInt();
            // 读取sql语句
            char[] sqlChars = new char[sqlCharLen];
            for (int i = 0; i < sqlCharLen; i++) {
                sqlChars[i] = in.readChar();
            }
            String sql = new String(sqlChars);
            System.out.println("数据库id:"+ databaseId +"接收到SQL:" + sql);
            try {
                ConnectSession connectSession = new ConnectSession("xmz", databaseId);
                SqlParser sqlParser = new SqlParser(connectSession);
                Command command = sqlParser.prepareCommand(sql);
                QueryResult queryResult = command.execCommand();

                OkPacket okPacket = new OkPacket(1, queryResult.toString());
                byte[] bytes = okPacket.getBytes();
                out.writeInt(bytes.length);
                out.write(OkPacket.PACKET_TYPE_OK);
                out.write(bytes);
            } catch (Exception e) {
                ErrPacket errPacket = new ErrPacket(1, e.getMessage());
                byte[] bytes = errPacket.getBytes();
                out.writeInt(bytes.length);
                out.write(OkPacket.PACKET_TYPE_ERR);
                out.write(bytes);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
