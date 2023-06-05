package com.moyu.test.store.metadata.obj;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class Column {

    private String columnName;

    private byte columnType;

    private int columnIndex;

    private int columnLength;

    private byte isPrimaryKey;

    private Object value;


    public Column(String columnName, byte columnType, int columnIndex, int columnLength) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnIndex = columnIndex;
        this.columnLength = columnLength;
        this.isPrimaryKey = 0;
    }


    public Column createNullValueColumn() {
        Column column = new Column(columnName, columnType, columnIndex, columnLength);
        column.setIsPrimaryKey(isPrimaryKey);
        column.setValue(null);
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

    @Override
    public boolean equals(Object o) {
        Column c = (Column) o;
        return value != null && value.equals(c.getValue());
    }

    @Override
    public int hashCode() {
        return (value != null ? value.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "Column{" +
                "columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                ", columnIndex=" + columnIndex +
                ", columnLength=" + columnLength +
                ", value=" + value +
                '}';
    }
}
