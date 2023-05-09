package com.moyu.test.store.metadata.obj;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class ColumnDto {

    private String columnName;

    private byte columnType;

    private int columnIndex;

    private int columnLength;


    public ColumnDto(String columnName, byte columnType, int columnIndex, int columnLength) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnIndex = columnIndex;
        this.columnLength = columnLength;
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
}
