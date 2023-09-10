package com.moyu.test.store.type.dbtype;

import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.WriteBuffer;
import com.moyu.test.store.type.DataType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public abstract class AbstractColumnType<T> implements DataType<T> {


    @Override
    public T read(ByteBuffer byteBuffer) {
        // 判断标记位，0表示值为null、1不为null
        byte flag = byteBuffer.get();
        if (flag == (byte) 0) {
            return null;
        } else {
            return readValue(byteBuffer);
        }
    }

    @Override
    public void write(WriteBuffer writeBuffer, T value) {
        // 如果是空值(null)，写入标记位值0
        if (value == null) {
            byte flag = 0;
            writeBuffer.put(flag);
        } else {
            byte flag = 1;
            writeBuffer.put(flag);
            writeValue(writeBuffer, value);
        }
    }


    @Override
    public int getMaxByteSize(T value) {
        // 1字节空值标记
        return getMaxByteLen(value) + 1;
    }

    abstract int getMaxByteLen(T value);


    @Override
    public int compare(T a, T b) {
        if(a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable)a).compareTo(((Comparable)b));
        } else {
            throw new UnsupportedOperationException("不支持compare方法");
        }
    }

    /**
     * 把值写入ByteBuffer
     * @return
     */
    protected abstract T readValue(ByteBuffer byteBuffer);

    protected abstract void writeValue(WriteBuffer writeBuffer, T value);


    public static DataType getDataType(byte columnType) {
        switch (columnType) {
            case DbColumnTypeConstant.INT_4:
                return new IntColumnType();
            case DbColumnTypeConstant.INT_8:
                return new LongColumnType();
            case DbColumnTypeConstant.VARCHAR:
            case DbColumnTypeConstant.CHAR:
                return new StringColumnType();
            case DbColumnTypeConstant.TIMESTAMP:
                return new TimeColumnType();
            default:
                throw new DbException("不支持数据类型:" + columnType);
        }
    }


}
