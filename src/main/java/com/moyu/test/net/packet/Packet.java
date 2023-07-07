package com.moyu.test.net.packet;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class Packet {
    /**
     * 整个数据包长度
     */
    protected int packetLen;
    /**
     * 包类型
     * 0 ErrPacket、1 OkPacket
     */
    protected byte packetType;


}
