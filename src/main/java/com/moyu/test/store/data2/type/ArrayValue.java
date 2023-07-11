package com.moyu.test.store.data2.type;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.WriteBuffer;
import com.moyu.test.store.type.obj.ArrayDataType;
import com.moyu.test.store.type.DataType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/6/27
 */
public class ArrayValue<V extends Value> extends Value {

    private DataType itemDataType;

    private V[] arr;


    public ArrayValue(V[] arr, DataType itemDataType) {
        this.arr = arr;
        this.itemDataType = itemDataType;
    }


    public ArrayValue(ByteBuffer byteBuffer) {
        byte flag = byteBuffer.get();
        int itemType = byteBuffer.getInt();
        DataType dataType = Value.getDataTypeObj(itemType);
        this.itemDataType = dataType;
        if (flag == (byte) 0) {
            arr = null;
        } else {
            int len = byteBuffer.getInt();
            Value[] array = new Value[len];
            int i = 0;
            while (i < len) {
                Object objValue = this.itemDataType.read(byteBuffer);
                Value value = createValue(objValue);
                array[i++] = value;
            }
            this.arr = (V[]) array;
        }

    }


    private Value createValue(Object value) {
        if (value instanceof Integer) {
            return new IntegerValue((Integer) value);
        } else if (value instanceof Long) {
            return new LongValue((Long) value);
        } else if (value instanceof String) {
            return new StringValue((String) value);
        } else {
            throw new DbException("不支持类型" + value);
        }

    }


    @Override
    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        int dataType = getDataType(itemDataType);
        if (arr == null) {
            writeBuffer.put((byte) 0);
            writeBuffer.putInt(dataType);
        } else {
            writeBuffer.put((byte) 1);
            writeBuffer.putInt(dataType);
            writeBuffer.putInt(arr.length);
            int i = 0;
            while (i < arr.length) {
                itemDataType.write(writeBuffer, arr[i++].getObjValue());
            }
        }
        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }

    @Override
    public DataType getDataTypeObj() {
        return new ArrayDataType();
    }

    @Override
    public int getType() {
        return Value.TYPE_VALUE_ARR;
    }

    @Override
    public Object getObjValue() {
        return null;
    }

    @Override
    public int compare(Value v) {
        return 0;
    }

    public V[] getArr() {
        return arr;
    }
}
