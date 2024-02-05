package com.moyu.xmz.store.type.value;

import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.IntType;
import com.moyu.xmz.store.type.dbtype.LongType;
import com.moyu.xmz.store.type.dbtype.StringType;
import com.moyu.xmz.store.type.obj.ArrayDataType;
import com.moyu.xmz.store.type.obj.RowDataType;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.SerializableByte;

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
            case DbTypeConstant.INT_4:
                return TYPE_INT;
            case DbTypeConstant.INT_8:
                return TYPE_LONG;
            case DbTypeConstant.CHAR:
            case DbTypeConstant.VARCHAR:
                return TYPE_STRING;
            case DbTypeConstant.TIMESTAMP:
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
                return new IntType();
            case TYPE_LONG:
                return new LongType();
            case TYPE_STRING:
                return new StringType();
            case TYPE_ROW_VALUE:
                return new RowDataType();
            case TYPE_VALUE_ARR:
                return new ArrayDataType();
            default:
                throw new DbException("不支持读取类型:" + type);
        }
    }

    public static int getDataType(DataType type) {
        if(type instanceof IntType){
            return TYPE_INT;
        }else if(type instanceof LongType){
            return TYPE_LONG;
        }else if(type instanceof StringType){
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
