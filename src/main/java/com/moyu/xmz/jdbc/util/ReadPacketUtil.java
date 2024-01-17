package com.moyu.xmz.jdbc.util;

import com.moyu.xmz.net.packet.ErrPacket;
import com.moyu.xmz.net.packet.OkPacket;
import com.moyu.xmz.net.packet.Packet;
import com.moyu.xmz.common.exception.DbException;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/28
 */
public class ReadPacketUtil {

    public static Packet readPacket(DataInputStream dataInputStream) throws IOException {

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
