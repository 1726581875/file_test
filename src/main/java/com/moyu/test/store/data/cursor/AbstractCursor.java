package com.moyu.test.store.data.cursor;

/**
 * @author xiaomingzhang
 * @date 2023/6/11
 */
public abstract class AbstractCursor implements Cursor {

    protected boolean closed;

    @Override
    public void close() {
        closed = true;
        closeCursor();
    }

    abstract void closeCursor();

}
