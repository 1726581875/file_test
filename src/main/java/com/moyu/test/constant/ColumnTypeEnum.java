package com.moyu.test.constant;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public enum ColumnTypeEnum {
    INT("int", DbColumnTypeConstant.INT_4),
    INT_4("int4", DbColumnTypeConstant.INT_4),
    BIGINT("bigint", DbColumnTypeConstant.INT_8),
    CHAR("char", DbColumnTypeConstant.CHAR),
    VARCHAR("varchar", DbColumnTypeConstant.VARCHAR),
    TIMESTAMP("timestamp", DbColumnTypeConstant.TIMESTAMP)
    ;
    private String typeName;

    private Byte columnType;

    ColumnTypeEnum(String columnTypeName, Byte columnType) {
        this.typeName = columnTypeName;
        this.columnType = columnType;
    }


    public static Byte getColumnTypeByName(String typeName){
        for (ColumnTypeEnum typeEnum : ColumnTypeEnum.values()) {
            if(typeEnum.getTypeName().equals(typeName)) {
                return typeEnum.getColumnType();
            }
        }
        return null;
    }

    public static String getNameByType(Byte columnType){
        for (ColumnTypeEnum typeEnum : ColumnTypeEnum.values()) {
            if(typeEnum.getColumnType().equals(columnType)) {
                return typeEnum.getTypeName();
            }
        }
        return null;
    }

    public String getTypeName() {
        return typeName;
    }

    public Byte getColumnType() {
        return columnType;
    }
}
