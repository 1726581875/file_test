package com.moyu.test.store.metadata.obj;

import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.constant.ColumnTypeEnum;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/22
 */
public class SelectColumn {

    /**
     * 列字段
     */
    private Column column;
    /**
     * 字段名
     */
    private String selectColumnName;

    /**
     * 字段别名
     */
    private String alias;

    private String tableAlias;

    /**
     * 函数名
     * 如count、sum、max、min等等
     */
    private String functionName;
    /**
     * 函数参数
     */
    private String[] args;


    public SelectColumn(Column column, String selectColumnName, String functionName, String[] args) {
        this.column = column;
        this.selectColumnName = selectColumnName;
        this.functionName = functionName;
        this.args = args;
    }


    public String getTableAliasColumnName() {
       return column.getTableAliasColumnName();
    }


    public static Column[] getColumnBySelectColumn(Query query) {
        List<Column> columnList = new ArrayList<>();
        int i = 0;
        for (SelectColumn selectColumn : query.getSelectColumns()) {
            String functionName = selectColumn.getFunctionName();
            if (functionName != null) {
                String columnName = selectColumn.getAlias();
                if (columnName == null) {
                    columnName = selectColumn.getSelectColumnName();
                }
                Column column = new Column(columnName, ColumnTypeEnum.INT_4.getColumnType(), i, 4);
                columnList.add(column);
            } else {
                Column column = selectColumn.getColumn().copy();
                column.setColumnIndex(i);
                columnList.add(column);
            }
            i++;
        }
        return columnList.toArray(new Column[0]);
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public String getSelectColumnName() {
        return selectColumnName;
    }

    public void setSelectColumnName(String selectColumnName) {
        this.selectColumnName = selectColumnName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String[] getArgs() {
        return args;
    }

    public void setArgs(String[] args) {
        this.args = args;
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
}
