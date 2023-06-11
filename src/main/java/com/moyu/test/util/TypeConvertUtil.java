package com.moyu.test.util;

import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.exception.DbException;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class TypeConvertUtil {


    public static Object convertValueType(String value, byte columnType) {
        Class<?> typeClass = ColumnTypeEnum.getJavaTypeClass(columnType);
        if (Integer.class.equals(typeClass)) {
            return Integer.valueOf(value);
        }
        if (Long.class.equals(typeClass)) {
            return Long.valueOf(value);
        }
        if (String.class.equals(typeClass)) {
            return value;
        } else {
            throw new DbException("类型不支持");
        }
    }

}
