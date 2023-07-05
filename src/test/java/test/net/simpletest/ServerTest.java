package test.net.simpletest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;


/**
 * @author xiaomingzhang
 * @date 2023/7/4
 */
public class ServerTest {


    public static void main(String[] args) {
        try {
            // 创建ServerSocket对象，并指定端口号
            ServerSocket serverSocket = new ServerSocket(8888);

            System.out.println("服务端已启动，等待客户端连接...");

            // 监听客户端连接，accept() 方法会阻塞直到有客户端连接
            Socket socket = serverSocket.accept();

            System.out.println("客户端已连接");

            // 获取输入流和输出流
            InputStream inputStream = socket.getInputStream();
            OutputStream outputStream = socket.getOutputStream();

            // 读取客户端发送的消息
            byte[] buffer = new byte[1024];
            int length = inputStream.read(buffer);
            String clientMessage = new String(buffer, 0, length);
            System.out.println("客户端消息：" + clientMessage);

            // 发送响应消息给客户端
            String responseMessage = "Hello, Client!";
            outputStream.write(responseMessage.getBytes());

            // 关闭资源
            outputStream.close();
            inputStream.close();
            socket.close();
            serverSocket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
