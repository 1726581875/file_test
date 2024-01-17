package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.store.common.WriteBuffer;
import com.moyu.xmz.common.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class TimeColumnType extends AbstractColumnType<Date> {

    @Override
    protected Date readValue(ByteBuffer byteBuffer) {
        long l = DataUtils.readLong(byteBuffer);
        return new Date(l);
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Date value) {
        writeBuffer.putLong(value.getTime());
    }

    @Override
    public Class<?> getValueTypeClass() {
        return Date.class;
    }

    @Override
    public int getMaxByteLen(Date value) {
        return 8;
    }
}
