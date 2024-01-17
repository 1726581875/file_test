package test.net.simpletest02;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;


/**
 * @author xiaomingzhang
 * @date 2023/7/5
 */
public class ClientTest02 {


    public static void main(String[] args) {
        try {
            // 创建Socket对象，并指定服务端IP地址和端口号
            Socket socket = new Socket("localhost", 8888);

            // 获取输入流和输出流
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();


            // 发送消息给服务端
            int dbId = 10;
            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
            byteBuffer.putInt(dbId);
            byteBuffer.flip();

            byte[] idBytes = new byte[4];
            byteBuffer.get(idBytes);
            outputStream.write(idBytes);

            // 发送消息给服务端
            String message = "Hello, Server!";
            outputStream.write(message.getBytes());

            // 读取服务端的响应消息
            byte[] buffer = new byte[1024];
            int length = inputStream.read(buffer);
            String serverMessage = new String(buffer, 0, length);
            System.out.println("服务端消息：" + serverMessage);

            // 关闭资源
            outputStream.close();
            inputStream.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
