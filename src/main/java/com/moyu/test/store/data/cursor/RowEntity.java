package com.moyu.test.store.data.cursor;

import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public class RowEntity {

    private Column[] columns;

    public RowEntity(Column[] columns) {
        this.columns = columns;
    }

    public Column[] getColumns() {
        return columns;
    }

    public Column getColumn(String columnName, String tableAlias) {
        for (Column c : columns) {
            if((tableAlias == null || tableAlias.equals(c.getTableAlias()))
                    && c.getColumnName().equals(columnName)) {
                return c;
            }
        }
        throw new SqlIllegalException("字段" + columnName + "不存在");
    }



    public static RowEntity mergeRow(RowEntity leftRow, RowEntity rightRow) {
        Column[] columns = Column.mergeColumns(leftRow.getColumns(), rightRow.getColumns());
        return new RowEntity(columns);
    }

}
