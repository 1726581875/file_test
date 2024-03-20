package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
public class NullColumnExpr extends SelectColumnExpr {

    @Override
    public Object getValue(RowEntity rowEntity) {
        return null;
    }
}
