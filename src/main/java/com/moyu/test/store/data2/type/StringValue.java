package com.moyu.test.store.data2.type;

import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.dbtype.StringColumnType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/9
 */
public final class StringValue extends Value {

    private String value;

    public StringValue(String value) {
        this.value = value;
    }

    @Override
    public DataType getDataTypeObj() {
        return new StringColumnType();
    }

    @Override
    public int getType() {
        return Value.TYPE_STRING;
    }

    @Override
    public Object getObjValue() {
        return value;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return null;
    }
}
