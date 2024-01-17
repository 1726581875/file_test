package com.moyu.xmz.common.exception;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class SqlQueryException extends DbException {

    public SqlQueryException(String msg) {
        super(msg);
    }
}
