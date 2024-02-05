package com.moyu.xmz.store.type;

import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.store.type.dbtype.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class ColumnTypeFactory {


    private static final Map<Byte, DataType> columnTypeMap = new HashMap<>();

    static {
        columnTypeMap.put(ColumnTypeConstant.TINY_INT, new TinyIntType());
        columnTypeMap.put(ColumnTypeConstant.INT_4, new IntType());
        columnTypeMap.put(ColumnTypeConstant.INT_8, new LongType());
        columnTypeMap.put(ColumnTypeConstant.VARCHAR, new StringType());
        columnTypeMap.put(ColumnTypeConstant.CHAR, new StringType());
        columnTypeMap.put(ColumnTypeConstant.TIMESTAMP, new DateType());
        columnTypeMap.put(ColumnTypeConstant.UNSIGNED_INT_4, new UnsignedIntType());
        columnTypeMap.put(ColumnTypeConstant.UNSIGNED_INT_8, new UnsignedLongType());
    }

    public static DataType getColumnType(Byte type) {
        DataType columnType = columnTypeMap.get(type);
        if (columnType == null) {
            throw new UnsupportedOperationException("不支持该操作,type=" + type);
        }
        return columnType;
    }





}
