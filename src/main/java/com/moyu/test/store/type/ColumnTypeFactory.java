package com.moyu.test.store.type;

import com.moyu.test.constant.DbColumnTypeConstant;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class ColumnTypeFactory {


    private static final Map<Byte, ColumnType> columnTypeMap = new HashMap<>();

    static {
        columnTypeMap.put(DbColumnTypeConstant.INT_4, new IntColumnType());
        columnTypeMap.put(DbColumnTypeConstant.INT_8, new LongColumnType());
        columnTypeMap.put(DbColumnTypeConstant.VARCHAR, new StringColumnType());
        columnTypeMap.put(DbColumnTypeConstant.CHAR, new StringColumnType());
        columnTypeMap.put(DbColumnTypeConstant.TIMESTAMP, new DateColumnType());
    }

    public static ColumnType getColumnType(Byte type) {
        ColumnType columnType = columnTypeMap.get(type);
        if (columnType == null) {
            throw new UnsupportedOperationException("不支持该操作,type=" + type);
        }
        return columnType;
    }





}
