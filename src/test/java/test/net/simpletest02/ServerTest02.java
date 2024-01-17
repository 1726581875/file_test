package test.net.simpletest02;

import com.moyu.xmz.common.util.DataUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;


/**
 * @author xiaomingzhang
 * @date 2023/7/4
 */
public class ServerTest02 {


    private static boolean stop = false;

    public static void main(String[] args) {
        try {
            // 创建ServerSocket对象，并指定端口号
            ServerSocket serverSocket = new ServerSocket(8888);

            System.out.println("服务端已启动，等待客户端连接...");


            while (!stop) {
                // 监听客户端连接，accept() 方法会阻塞直到有客户端连接
                Socket socket = serverSocket.accept();
                System.out.println("客户端已连接");
                // 获取输入流和输出流
                InputStream inputStream = socket.getInputStream();
                OutputStream outputStream = socket.getOutputStream();
                try {

                    byte[] bytes = new byte[4];
                    inputStream.read(bytes);

                    ByteBuffer dbIdBytes = ByteBuffer.allocate(4);
                    dbIdBytes.put(bytes);
                    dbIdBytes.rewind();
                    int dbId = DataUtils.readInt(dbIdBytes);
                    System.out.println("databaseId=" + dbId);

                    // 读取客户端发送的消息
                    byte[] buffer = new byte[1024];
                    int length = inputStream.read(buffer);
                    String clientMessage = new String(buffer, 0, length);
                    System.out.println("客户端消息：" + clientMessage);

                    // 发送响应消息给客户端
                    String responseMessage = "Hello, Client!";
                    outputStream.write(responseMessage.getBytes());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    socket.close();
                    outputStream.close();
                    inputStream.close();
                }


            }
            serverSocket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
