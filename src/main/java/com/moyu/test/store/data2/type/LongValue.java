package com.moyu.test.store.data2.type;

import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.dbtype.LongColumnType;

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
        return new LongColumnType();
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
