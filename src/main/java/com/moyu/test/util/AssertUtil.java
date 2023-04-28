package com.moyu.test.util;

import com.moyu.test.exception.AssertException;

/**
 * @author xiaomingzhang
 * @date 2023/4/28
 * 断言工具类
 */
public class AssertUtil {

    public static void isNUll(Object obj) {
        assertTrue(obj == null, "对象必须为空");
    }

    public static void assertTrue(boolean condition, String msg) {
        if (!condition) {
            throw new AssertException(msg);
        }
    }

}
