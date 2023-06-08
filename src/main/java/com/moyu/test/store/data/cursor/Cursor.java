package com.moyu.test.store.data.cursor;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public interface Cursor {

    RowEntity next();

    void reset();

}
