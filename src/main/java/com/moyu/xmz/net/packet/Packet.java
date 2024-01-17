package com.moyu.xmz.net.packet;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class Packet {

    public static final byte PACKET_TYPE_ERR = 0;
    public static final byte PACKET_TYPE_OK = 1;

    /**
     * 整个数据包长度,不包含自身和packetType
     */
    protected int packetLen;
    /**
     * 包类型
     * 0 ErrPacket、1 OkPacket
     */
    protected byte packetType;


    public byte getPacketType() {
        return packetType;
    }
}
