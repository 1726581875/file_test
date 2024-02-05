package com.moyu.xmz.common.util;

import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.exception.DbException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class TypeConvertUtils {


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
        }
        if (Date.class.equals(typeClass)) {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                return dateFormat.parse(value);
            } catch (Exception e) {
                e.printStackTrace();
                throw new DbException("日常格式转换发生异常");
            }
        } else {
            throw new DbException("类型不支持");
        }
    }

}
