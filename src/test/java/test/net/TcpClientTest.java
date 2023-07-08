package test.net;

import com.moyu.test.exception.DbException;
import com.moyu.test.net.packet.ErrPacket;
import com.moyu.test.net.packet.OkPacket;
import com.moyu.test.net.packet.Packet;

import java.io.*;
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
            DataInputStream dataInputStream = new DataInputStream(inputStream);

            // 发送数据库id给服务端
            int dbId = 3;;
            dataOutputStream.writeInt(dbId);

            // 发送sql给服务端
            String message = "select *1 from xmz_table";
            int length1 = message.length();
            dataOutputStream.writeInt(length1);
            dataOutputStream.writeChars(message);


            Packet packet = readPacket(dataInputStream);
            if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
                OkPacket okPacket = (OkPacket) packet;
                System.out.println("sql执行成功，结果:");
                System.out.println(okPacket.getResultStr());
            } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
                ErrPacket errPacket = (ErrPacket) packet;
                System.out.println("sql执行失败,错误码: " + errPacket.getErrCode() + "，错误信息" + errPacket.getErrMsg());
            } else {
                System.out.println("不支持的packet type" + packet.getPacketType());
            }

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


    private static Packet readPacket(DataInputStream dataInputStream) throws IOException {

        Packet packet = null;

        int packetLen = dataInputStream.readInt();
        byte packetType = dataInputStream.readByte();

        ByteBuffer byteBuffer = ByteBuffer.allocate(packetLen);
        byte[] bytes = new byte[1024];
        while (byteBuffer.remaining() > 0) {
            int read = dataInputStream.read(bytes);
            if (read == -1) {
                throw new EOFException();
            }
            byteBuffer.put(bytes, 0, read);
        }
        byteBuffer.flip();

        if (packetType == Packet.PACKET_TYPE_OK) {
            packet = new OkPacket(byteBuffer);
        } else if (packetType == Packet.PACKET_TYPE_ERR) {
            packet = new ErrPacket(byteBuffer);
        } else {
            throw new DbException("不支持的packet type " + packetType);
        }


        return packet;
    }





}
