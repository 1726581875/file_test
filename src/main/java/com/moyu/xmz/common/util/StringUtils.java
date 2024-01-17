package com.moyu.xmz.common.util;

/**
 * @author xiaomingzhang
 * @date 2023/8/22
 */
public class StringUtils {


    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }


}
