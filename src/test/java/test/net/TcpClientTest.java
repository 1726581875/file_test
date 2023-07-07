package test.net;

import java.io.*;
import java.net.Socket;

/**
 * @author xiaomingzhang
 * @date 2023/7/6
 */
public class TcpClientTest {

    public static void main(String[] args) {
        try {
            // 创建Socket对象，并指定服务端IP地址和端口号
            Socket socket = new Socket("localhost", 8888);
            // 获取输入流和输出流
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
            DataInputStream dataInputStream = new DataInputStream(inputStream);

            // 发送数据库id给服务端
            int dbId = 3;;
            dataOutputStream.writeInt(dbId);

            // 发送sql给服务端
            String message = "select * from xmz_table";
            int length1 = message.length();
            dataOutputStream.writeInt(length1);
            dataOutputStream.writeChars(message);


            int resultCharLen = dataInputStream.readInt();
            char[] sqlChars = new char[resultCharLen];
            for (int i = 0; i < resultCharLen; i++) {
                sqlChars[i] = dataInputStream.readChar();
            }
            System.out.println(new String(sqlChars));

            // 关闭资源
            dataOutputStream.close();
            outputStream.close();
            dataInputStream.close();
            inputStream.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
