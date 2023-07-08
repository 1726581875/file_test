package com.moyu.test.net.packet;

import com.moyu.test.store.WriteBuffer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class ErrPacket extends Packet {

    private int errCode;

    private String errMsg;

    public ErrPacket(int errCode, String errMsg) {
        super.packetType = Packet.PACKET_TYPE_ERR;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }


    public ErrPacket(ByteBuffer buffer) {
        super.packetLen = buffer.limit();
        super.packetType = Packet.PACKET_TYPE_ERR;
        this.errCode = buffer.getInt();
        int msgLen = buffer.getInt();
        StringBuilder builder = new StringBuilder();
        while (msgLen > 0) {
            builder.append(buffer.getChar());
            msgLen--;
        }
        this.errMsg = builder.toString();
    }


    public void write(DataOutputStream outputStream) throws IOException {
        outputStream.writeInt(super.packetLen);
        outputStream.writeByte(super.packetType);
        outputStream.writeInt(errCode);
        outputStream.writeInt(errMsg.length());
        outputStream.writeChars(errMsg);
    }


    public byte[] getBytes() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.putInt(errCode);
        writeBuffer.putInt(errMsg.length());

        int i = 0;
        char[] charArray = errMsg.toCharArray();
        while (i < errMsg.length()) {
            writeBuffer.putChar(charArray[i++]);
        }
        ByteBuffer buffer = writeBuffer.getBuffer();
        int packetLength = buffer.position();
        buffer.flip();
        byte[] bytes = new byte[packetLength];
        buffer.get(bytes);
        return bytes;
    }


    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }
}
