package com.moyu.test.store.data.cursor;

import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public interface Cursor {

    RowEntity next();

    void reset();

    Column[] getColumns();

    void close();

    void reUse();

}
