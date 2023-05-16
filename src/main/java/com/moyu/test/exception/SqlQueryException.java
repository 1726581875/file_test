package com.moyu.test.exception;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class SqlQueryException extends RuntimeException {

    public SqlQueryException(String msg) {
        super(msg);
    }
}
