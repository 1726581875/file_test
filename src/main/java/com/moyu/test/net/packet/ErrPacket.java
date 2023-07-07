package com.moyu.test.net.packet;


import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class ErrPacket extends Packet {

    private int errCode;

    private String errMsg;

    public ErrPacket(int errCode, String errMsg) {
        super.packetType = 0;
        super.packetLen = 0;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }




    public void write(DataOutputStream outputStream) throws IOException {
        outputStream.writeInt(super.packetLen);
        outputStream.writeByte(super.packetType);
        outputStream.writeInt(errCode);
        outputStream.writeInt(errMsg.length());
        outputStream.writeChars(errMsg);
    }
}
