package com.moyu.test.net.packet;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.store.metadata.obj.SelectColumn;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/7
 */
public class OkPacket extends Packet {

    /**
     * 操作类型
     * 0查找、1、新增、2、查找、3删除
     */
    private byte opType;
    /**
     * 受影响的行
     */
    private int affRows;
    /**
     * 查询结果行
     */
    private int resRows;
    /**
     * TODO 当前测试，所有结果作为一个字符串返回
     */
    private String resultStr;

    private SelectColumn[] columns;

    private List<Object[]> resultRows;


    public OkPacket(int resRows, String resultStr) {
        super.packetType = 1;
        this.resRows = resRows;
        this.resultStr = resultStr;
    }

    public OkPacket(ByteBuffer buffer) {
        super.packetLen = buffer.limit();
        super.packetType = Packet.PACKET_TYPE_OK;
        this.opType = buffer.get();
        this.affRows = buffer.getInt();
        this.resRows = buffer.getInt();
        int msgLen = buffer.getInt();
        StringBuilder builder = new StringBuilder();
        while (msgLen > 0) {
            builder.append(buffer.getChar());
            msgLen--;
        }
        this.resultStr = builder.toString();
    }



    public byte[] getBytes(){
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.put(opType);
        writeBuffer.putInt(affRows);
        writeBuffer.putInt(resRows);
        writeBuffer.putInt(resultStr.length());
        int i = 0;
        char[] charArray = resultStr.toCharArray();
        while (i < resultStr.length()) {
            writeBuffer.putChar(charArray[i++]);
        }
        ByteBuffer buffer = writeBuffer.getBuffer();
        int packetLength = buffer.position();
        buffer.flip();
        byte[] bytes = new byte[packetLength];
        buffer.get(bytes);
        return bytes;
    }


    public String getResultStr() {
        return resultStr;
    }
}
