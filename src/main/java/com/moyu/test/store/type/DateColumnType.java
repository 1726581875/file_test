package com.moyu.test.store.type;

import com.moyu.test.store.WriteBuffer;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;
import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/5/19
 */
public class DateColumnType extends AbstractColumnType<Date> {

    @Override
    protected Date readValue(ByteBuffer byteBuffer) {
        long time = DataUtils.readLong(byteBuffer);
        return new Date(time);
    }

    @Override
    protected void writeValue(WriteBuffer writeBuffer, Date value) {
        writeBuffer.putLong(value.getTime());
    }

}
