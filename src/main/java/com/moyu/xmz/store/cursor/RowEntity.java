package com.moyu.xmz.store.cursor;

import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.exception.SqlIllegalException;
import com.moyu.xmz.common.DynByteBuffer;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.type.ColumnTypeFactory;
import com.moyu.xmz.store.type.DataType;

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

    public Column getColumns(int i) {
        if(i >= columns.length) {
            ExceptionUtil.throwDbException("获取列字段发生异常，下标超出限制,length:，index:{}",columns.length, i);
        }
        return columns[i];
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

    public void setRowId(Long rowId) {
        this.rowId = rowId;
    }


    public byte[] getColumnDataBytes() {
        DynByteBuffer buffer = new DynByteBuffer(16);
        for (Column column : this.columns) {
            DataType columnType = ColumnTypeFactory.getColumnType(column.getColumnType());
            columnType.write(buffer, column.getValue());
        }
        return buffer.flipAndGetBytes();
    }

    @Override
    public String toString() {
        return "RowEntity{" +
                "columns=" + Arrays.toString(columns) +
                '}';
    }
}
