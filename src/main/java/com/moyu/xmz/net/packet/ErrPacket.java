package com.moyu.xmz.net.packet;

import com.moyu.xmz.net.util.ReadWriteUtil;
import com.moyu.xmz.common.DynByteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class ErrPacket extends Packet {

    private int errCode;

    private String errMsg;

    public ErrPacket(int errCode, String errMsg) {
        super.packetType = PACKET_TYPE_ERR;
        this.errCode = errCode;
        this.errMsg = errMsg;
    }


    public ErrPacket(ByteBuffer buffer) {
        super.packetLen = buffer.limit();
        super.packetType = PACKET_TYPE_ERR;
        this.errCode = buffer.getInt();
        this.errMsg = ReadWriteUtil.readString(buffer);
    }


    public byte[] getBytes() {
        DynByteBuffer buffer = new DynByteBuffer();
        buffer.putInt(errCode);
        ReadWriteUtil.writeString(buffer, errMsg);
        return buffer.flipAndGetBytes();
    }


    public int getErrCode() {
        return errCode;
    }

    public String getErrMsg() {
        return errMsg;
    }
}
