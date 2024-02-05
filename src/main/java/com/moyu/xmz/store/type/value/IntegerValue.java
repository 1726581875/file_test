package com.moyu.xmz.store.type.value;

import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.IntType;

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
        return new IntType();
    }

    @Override
    public int getType() {
        return TYPE_INT;
    }

    @Override
    public Object getObjValue() {
        return value;
    }

    @Override
    public int compare(Value v) {
        return value.compareTo(((IntegerValue) v).getValue());
    }

    @Override
    public int getMaxSize() {
        return 4;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return null;
    }

    public Integer getValue() {
        return value;
    }
}
