package com.moyu.test.store.transaction;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public interface TxOperator {

    void begin();

    void commit();

    void rollback();

}
