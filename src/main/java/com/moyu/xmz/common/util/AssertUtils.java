package com.moyu.xmz.common.util;

import com.moyu.xmz.common.exception.AssertException;

/**
 * @author xiaomingzhang
 * @date 2023/4/28
 * 断言工具类
 */
public class AssertUtils {

    public static void isNUll(Object obj) {
        assertTrue(obj == null, "对象必须为空");
    }

    public static void assertTrue(boolean condition, String msg) {
        if (!condition) {
            throw new AssertException(msg);
        }
    }

}
