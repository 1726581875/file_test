package com.moyu.xmz.net.model.terminal;

import com.moyu.xmz.net.model.BaseResultDto;
import com.moyu.xmz.net.util.ReadWriteUtil;
import com.moyu.xmz.store.common.WriteBuffer;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/9/16
 */
public class QueryResultStrDto implements BaseResultDto {

    private int totalByteLen;

    /**
     * 查询结果格式化字符串
     */
    private String resultStr;

    public QueryResultStrDto(String resultStr) {
        this.resultStr = resultStr;
    }


    public QueryResultStrDto(ByteBuffer byteBuffer) {
        this.totalByteLen = byteBuffer.getInt();
        this.resultStr = ReadWriteUtil.readString(byteBuffer);
    }


    @Override
    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.putInt(totalByteLen);
        ReadWriteUtil.writeString(writeBuffer, resultStr);
        totalByteLen = writeBuffer.position();
        writeBuffer.putInt(0, totalByteLen);
        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }
}
