package com.moyu.test.exception;

/**
 * @author xiaomingzhang
 * @date 2023/5/18
 */
public class SqlExecutionException extends RuntimeException {

    public SqlExecutionException(String msg) {
        super(msg);
    }
}
