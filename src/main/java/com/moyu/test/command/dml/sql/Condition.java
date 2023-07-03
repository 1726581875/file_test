package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public interface Condition {

    boolean getResult(RowEntity row);

    void close();

}
