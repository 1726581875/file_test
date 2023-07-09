package com.moyu.test.store.data.cursor;

import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.metadata.obj.Column;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public class RowEntity {

    private Long rowId;

    private boolean isDeleted;

    private Column[] columns;

    public RowEntity(Column[] columns) {
        this.columns = columns;
    }

    public RowEntity(Column[] columns, long rowId, boolean isDeleted) {
        this.columns = columns;
        this.rowId = rowId;
        this.isDeleted = isDeleted;
    }

    public RowEntity(Column[] columns, String tableAlias) {

        Column[] arr = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
            arr[i] = columns[i].copy();
            arr[i].setTableAlias(tableAlias);
        }
        this.columns = arr;
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

        String tbAlias = tableAlias == null ? "" : tableAlias + ".";
        throw new SqlIllegalException("字段" + tbAlias + columnName + "不存在");
    }

    public RowEntity setTableAlias(String tableAlias){
        for (int i = 0; i < columns.length; i++) {
            columns[i].setTableAlias(tableAlias);
        }
        return this;
    }


    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public static RowEntity mergeRow(RowEntity leftRow, RowEntity rightRow) {
        Column[] columns = Column.mergeColumns(leftRow.getColumns(), rightRow.getColumns());
        return new RowEntity(columns);
    }


    public Long getRowId() {
        return rowId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowEntity rowEntity = (RowEntity) o;
        return isDeleted == rowEntity.isDeleted && Arrays.equals(columns, rowEntity.columns);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(isDeleted);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }

    @Override
    public String toString() {
        return "RowEntity{" +
                "columns=" + Arrays.toString(columns) +
                '}';
    }
}
