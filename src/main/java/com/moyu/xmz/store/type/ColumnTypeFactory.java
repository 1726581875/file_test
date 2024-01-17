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
        columnTypeMap.put(ColumnTypeConstant.TINY_INT, new TinyIntColumnType());
        columnTypeMap.put(ColumnTypeConstant.INT_4, new IntColumnType());
        columnTypeMap.put(ColumnTypeConstant.INT_8, new LongColumnType());
        columnTypeMap.put(ColumnTypeConstant.VARCHAR, new StringColumnType());
        columnTypeMap.put(ColumnTypeConstant.CHAR, new StringColumnType());
        columnTypeMap.put(ColumnTypeConstant.TIMESTAMP, new DateColumnType());
        columnTypeMap.put(ColumnTypeConstant.UNSIGNED_INT_4, new UnsignedIntColumnType());
        columnTypeMap.put(ColumnTypeConstant.UNSIGNED_INT_8, new UnsignedLongColumnType());
    }

    public static DataType getColumnType(Byte type) {
        DataType columnType = columnTypeMap.get(type);
        if (columnType == null) {
            throw new UnsupportedOperationException("不支持该操作,type=" + type);
        }
        return columnType;
    }





}
