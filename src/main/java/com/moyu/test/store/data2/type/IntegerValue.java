package com.moyu.test.store.data2.type;

import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.dbtype.IntColumnType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/7/9
 */
public class IntegerValue extends Value {

    private Integer value;


    public IntegerValue(Integer value) {
        this.value = value;
    }

    @Override
    public DataType getDataTypeObj() {
        return new IntColumnType();
    }

    @Override
    public int getType() {
        return Value.TYPE_INT;
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
