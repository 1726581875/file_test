package com.moyu.xmz.net.packet;

import com.moyu.xmz.net.constant.CmdTypeConstant;
import com.moyu.xmz.net.model.terminal.DatabaseInfo;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.net.model.BaseResultDto;
import com.moyu.xmz.net.model.terminal.QueryResultDto;
import com.moyu.xmz.net.model.terminal.QueryResultStrDto;
import com.moyu.xmz.store.common.WriteBuffer;

import java.nio.ByteBuffer;

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

    private byte commandType;

    private int contentLen;

    private BaseResultDto content;


    public OkPacket(int resRows, BaseResultDto content, byte commandType) {
        super.packetType = Packet.PACKET_TYPE_OK;
        this.resRows = resRows;
        this.content = content;
        this.commandType = commandType;
    }

    public OkPacket(ByteBuffer buffer) {
        super.packetLen = buffer.limit();
        super.packetType = Packet.PACKET_TYPE_OK;
        this.opType = buffer.get();
        this.affRows = buffer.getInt();
        this.resRows = buffer.getInt();
        this.commandType = buffer.get();
        this.contentLen = buffer.getInt();
        if (this.contentLen > 0) {
            switch (commandType) {
                case CmdTypeConstant.DB_INFO:
                    this.content = new DatabaseInfo(buffer);
                    break;
                case CmdTypeConstant.DB_QUERY:
                case CmdTypeConstant.DB_PREPARED_QUERY:
                case CmdTypeConstant.DB_QUERY_PAGE:
                    this.content = new QueryResultDto(buffer);
                    break;
                case CmdTypeConstant.DB_QUERY_RES_STR:
                    this.content = new QueryResultStrDto(buffer);
                    break;
                default:
                    ExceptionUtil.throwDbException("内容类型不合法,contentType:{}", commandType);

            }
        }
    }


    public byte[] getBytes() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.put(opType);
        writeBuffer.putInt(affRows);
        writeBuffer.putInt(resRows);
        writeBuffer.put(commandType);
        // 查询结果序列化
        if (content == null) {
            this.contentLen = 0;
            writeBuffer.putInt(this.contentLen);
        } else {
            ByteBuffer bf = content.getByteBuffer();
            this.contentLen = bf.limit();
            writeBuffer.putInt(this.contentLen);
            writeBuffer.put(bf);
        }

        ByteBuffer buffer = writeBuffer.getBuffer();
        int packetLength = buffer.position();
        buffer.flip();
        byte[] bytes = new byte[packetLength];
        buffer.get(bytes);
        return bytes;
    }

    public int getAffRows() {
        return affRows;
    }

    public BaseResultDto getContent() {
        return content;
    }
}
