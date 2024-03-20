package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
public class TableColumnExpr extends SelectColumnExpr {

    private Column tableColumn;


    public TableColumnExpr(Column tableColumn) {
        this.tableColumn = tableColumn;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        Column column = rowEntity.getColumn(this.tableColumn.getColumnName(), this.tableColumn.getTableAlias());
        if(column == null) {
            ExceptionUtil.throwSqlIllegalException("字段不存在:{}.{}",this.tableColumn.getTableAlias(), this.tableColumn.getColumnName());
        }
        return column;
    }
}
