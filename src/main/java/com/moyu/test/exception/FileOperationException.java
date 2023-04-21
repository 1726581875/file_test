package com.moyu.test.exception;

/**
 * @author xiaomingzhang
 * @date 2023/4/21
 */
public class FileOperationException extends RuntimeException {

    public FileOperationException(String msg) {
        super(msg);
    }
}
