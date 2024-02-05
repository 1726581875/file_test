package com.moyu.xmz.store.type.value;

import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.LongType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/9
 */
public class LongValue extends Value {

    private Long value;

    public LongValue(Long value) {
        this.value = value;
    }

    @Override
    public DataType getDataTypeObj() {
        return new LongType();
    }

    @Override
    public int getType() {
        return Value.TYPE_LONG;
    }

    @Override
    public Object getObjValue() {
        return value;
    }

    @Override
    public int compare(Value v) {
        return value.compareTo(((LongValue) v).getValue());
    }

    @Override
    public int getMaxSize() {
        return 8;
    }

    public Long getValue() {
        return value;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(value);
        byteBuffer.rewind();
        return byteBuffer;
    }
}
