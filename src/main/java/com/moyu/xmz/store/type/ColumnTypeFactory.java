package com.moyu.xmz.store.type;

import com.moyu.xmz.common.constant.DbTypeConstant;
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
        columnTypeMap.put(DbTypeConstant.TINY_INT, new TinyIntType());
        columnTypeMap.put(DbTypeConstant.INT_4, new IntType());
        columnTypeMap.put(DbTypeConstant.INT_8, new LongType());
        columnTypeMap.put(DbTypeConstant.VARCHAR, new StringType());
        columnTypeMap.put(DbTypeConstant.CHAR, new StringType());
        columnTypeMap.put(DbTypeConstant.TIMESTAMP, new DateType());
        columnTypeMap.put(DbTypeConstant.UNSIGNED_INT_4, new UnsignedIntType());
        columnTypeMap.put(DbTypeConstant.UNSIGNED_INT_8, new UnsignedLongType());
    }

    public static DataType getColumnType(Byte type) {
        DataType columnType = columnTypeMap.get(type);
        if (columnType == null) {
            throw new UnsupportedOperationException("不支持该操作,type=" + type);
        }
        return columnType;
    }





}
