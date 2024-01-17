package com.moyu.xmz.store.cursor;

import com.moyu.xmz.store.common.dto.Column;

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
