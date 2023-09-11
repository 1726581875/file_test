package com.moyu.test.store.data2.type;

import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.SerializableByte;
import com.moyu.test.store.type.dbtype.IntColumnType;
import com.moyu.test.store.type.dbtype.LongColumnType;
import com.moyu.test.store.type.dbtype.StringColumnType;
import com.moyu.test.store.type.obj.ArrayDataType;
import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.obj.RowDataType;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/6/27
 */
public abstract class Value implements SerializableByte {

    public static final int TYPE_INT = 1;
    public static final int TYPE_LONG = 2;
    public static final int TYPE_STRING = 3;
    public static final int TYPE_ROW_VALUE = 4;
    public static final int TYPE_VALUE_ARR = 5;


    public abstract DataType getDataTypeObj();

    public abstract int getType();

    public abstract Object getObjValue();

    public abstract int compare(Value v);

    /**
     * 获取最大占用字节大小
     * @return
     */
    public abstract int getMaxSize();


    public static int getValueTypeByColumnType(byte columnType) {
        switch (columnType) {
            case ColumnTypeConstant.INT_4:
                return TYPE_INT;
            case ColumnTypeConstant.INT_8:
                return TYPE_LONG;
            case ColumnTypeConstant.CHAR:
            case ColumnTypeConstant.VARCHAR:
                return TYPE_STRING;
            case ColumnTypeConstant.TIMESTAMP:
                return TYPE_LONG;
            default:
                throw new DbException("不支持读取类型:" + columnType);
        }
    }

    public static Value readValue(ByteBuffer byteBuffer, int type) {
        switch (type) {
            case TYPE_ROW_VALUE:
                return new RowValue(byteBuffer);
            case TYPE_VALUE_ARR:
                return new ArrayValue(byteBuffer);
            default:
                throw new DbException("不支持读取类型:" + type);
        }
    }



    public static DataType getDataTypeObj(int type) {
        switch (type) {
            case TYPE_INT:
                return new IntColumnType();
            case TYPE_LONG:
                return new LongColumnType();
            case TYPE_STRING:
                return new StringColumnType();
            case TYPE_ROW_VALUE:
                return new RowDataType();
            case TYPE_VALUE_ARR:
                return new ArrayDataType();
            default:
                throw new DbException("不支持读取类型:" + type);
        }
    }

    public static int getDataType(DataType type) {
        if(type instanceof IntColumnType){
            return TYPE_INT;
        }else if(type instanceof LongColumnType){
            return TYPE_LONG;
        }else if(type instanceof StringColumnType){
            return TYPE_STRING;
        } else if (type instanceof RowDataType) {
            return TYPE_ROW_VALUE;
        } else if(type instanceof ArrayDataType){
            return TYPE_VALUE_ARR;
        } else {
            throw new DbException("不支持数据类型:" + type);
        }
    }

}
