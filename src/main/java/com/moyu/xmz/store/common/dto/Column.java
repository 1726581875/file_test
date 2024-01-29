package com.moyu.xmz.store.common.dto;

import com.moyu.xmz.command.dml.sql.Parameter;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class Column {
    /**
     * 字段名
     */
    private String columnName;
    /**
     * 字段别名
     */
    private String alias;
    /**
     * 所属表别名
     */
    private String tableAlias;
    /**
     * 字段类型，见ColumnTypeConstant
     */
    private byte columnType;
    /**
     * 字段下标位置
     */
    private int columnIndex;
    /**
     * 字段长度
     */
    private int columnLength;
    /**
     * 是否是主键： 0否、1是
     */
    private byte isPrimaryKey;
    /**
     * 字段注释，允许空的字段
     */
    private String comment;
    /**
     * 是否不能为空，0否、1是
     */
    private byte isNotNull;
    /**
     * 字段默认值,如果是空或者NULL表示默认空
     * 如果有默认值，字符传、日期这类会带上单引号或者双引号
     */
    private String defaultVal;
    /**
     * 字段值
     */
    private Object value;


    public Column(String columnName, byte columnType, int columnIndex, int columnLength) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnIndex = columnIndex;
        this.columnLength = columnLength;
        this.isPrimaryKey = 0;
        this.isNotNull = 0;
    }


    public Column createNullValueColumn() {
        Column column = new Column(columnName, columnType, columnIndex, columnLength);
        column.setIsPrimaryKey(isPrimaryKey);
        column.setTableAlias(tableAlias);
        column.setAlias(alias);
        column.setValue(null);
        return column;
    }

    public Column copy() {
        Column column = new Column(columnName, columnType, columnIndex, columnLength);
        column.setIsPrimaryKey(isPrimaryKey);
        column.setTableAlias(tableAlias);
        column.setAlias(alias);
        column.setValue(value);
        return column;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public byte getColumnType() {
        return columnType;
    }

    public void setColumnType(byte columnType) {
        this.columnType = columnType;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public int getColumnLength() {
        return columnLength;
    }

    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    public Object getValue() {
        if(value != null && value instanceof Parameter) {
         return ((Parameter) value).getValue();
        }
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }


    public byte getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(byte isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public byte getIsNotNull() {
        return isNotNull;
    }

    public void setIsNotNull(byte isNotNull) {
        this.isNotNull = isNotNull;
    }

    public String getDefaultVal() {
        return defaultVal;
    }

    public void setDefaultVal(String defaultVal) {
        this.defaultVal = defaultVal;
    }

    @Override
    public boolean equals(Object o) {
        Column c = (Column) o;
        return value != null && value.equals(c.getValue());
    }

    public boolean metaEquals(Object o) {
        Column c = (Column) o;
        if(tableAlias != null) {
            if(!tableAlias.equals(c.getTableAlias())) {
                return false;
            }
        }
        return columnName.equals(c.getColumnName());
    }


    @Override
    public int hashCode() {
        return (value != null ? value.hashCode() : 0);
    }


    public static Column[] mergeColumns(Column[] mainColumns, Column[] joinColumns) {
        int l = mainColumns.length + joinColumns.length;
        Column[] allColumns = new Column[l];
        for (int i = 0; i < l; i++) {
            if (i < mainColumns.length) {
                allColumns[i] = mainColumns[i];
            } else {
                allColumns[i] = joinColumns[i - mainColumns.length];
            }
        }
        return allColumns;
    }

    public static void setColumnTableAlias(Column[] columns, String tableAlias) {
        for (Column c : columns) {
            c.setTableAlias(tableAlias);
        }
    }


    public String getTableAliasColumnName() {
        String tableAlias = this.tableAlias == null ? "" : this.tableAlias + ".";
        return tableAlias + columnName;
    }

    @Override
    public String toString() {
        return "Column{" +
                "columnName='" + columnName + '\'' +
                ", alias='" + alias + '\'' +
                ", tableAlias='" + tableAlias + '\'' +
                ", columnType=" + columnType +
                ", columnIndex=" + columnIndex +
                ", columnLength=" + columnLength +
                ", isPrimaryKey=" + isPrimaryKey +
                ", value=" + value +
                '}';
    }
}
