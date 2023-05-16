package com.moyu.test.exception;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class SqlIllegalException extends RuntimeException {

    public SqlIllegalException(String msg) {
        super(msg);
    }

}
