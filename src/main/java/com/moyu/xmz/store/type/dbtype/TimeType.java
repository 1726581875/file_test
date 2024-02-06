package com.moyu.xmz.store.type.dbtype;

import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class TimeType extends AbstractDbType<Date> {

    @Override
    protected Date readValue(ByteBuffer byteBuffer) {
        long l = DataByteUtils.readLong(byteBuffer);
        return new Date(l);
    }

    @Override
    protected void writeValue(DynByteBuffer buffer, Date value) {
        buffer.putLong(value.getTime());
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
