package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
public class NullColumnExpr extends SelectColumnExpr {

    @Override
    public Object getValue(RowEntity rowEntity) {
        Column dateColumn = new Column("NULL", DbTypeConstant.CHAR, -1, 0);
        dateColumn.setValue(null);
        return dateColumn;
    }
}
