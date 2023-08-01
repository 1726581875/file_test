package com.moyu.test.store.data2.type;

import com.moyu.test.store.type.DataType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/18
 */
public class BooleanValue extends Value {

    private Boolean value;

    public BooleanValue(Boolean value) {
        this.value = value;
    }

    @Override
    public DataType getDataTypeObj() {

        return null;
    }

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public Object getObjValue() {
        return value;
    }

    @Override
    public int compare(Value v) {
        return 0;
    }

    @Override
    public int getMaxSize() {
        return 1;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return null;
    }
}
