package test.net;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

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

            // 发送数据库id给服务端
            int dbId = 10;;
            dataOutputStream.writeInt(dbId);

            // 发送sql给服务端
            String message = "select * from xma_table";

            int length1 = message.length();
            dataOutputStream.writeInt(length1);
            dataOutputStream.writeChars(message);

            // 关闭资源
            dataOutputStream.close();
            outputStream.close();
            inputStream.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
