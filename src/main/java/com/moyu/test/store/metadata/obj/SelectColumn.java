package com.moyu.test.store.metadata.obj;

import com.moyu.test.command.dml.expression.SelectColumnExpression;
import com.moyu.test.command.dml.function.FuncArg;
import com.moyu.test.command.dml.function.StatFunction;
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
    /**
     * 表别名
     */
    private String tableAlias;
    /**
     * 结果字段类型
     */
    private Byte columnType;

    /**
     * 函数名
     * 如count、sum、max、min等等
     */
    private String functionName;
    /**
     * 函数参数
     */
    private String[] args;
    private List<FuncArg> funcArgs;

    private StatFunction statFunction;

    private SelectColumnExpression columnExpression;


    public SelectColumn(Column column, String selectColumnName, String functionName, String[] args) {
        this.column = column;
        this.selectColumnName = selectColumnName;
        this.functionName = functionName;
        this.args = args;
    }

    public static SelectColumn newColumn(String columnName, Byte columnType) {
        SelectColumn selectColumn = new SelectColumn(null, columnName, null, null);
        selectColumn.setColumnType(columnType);
        return selectColumn;
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
            } else if(selectColumn.getColumn() != null) {
                Column column = selectColumn.getColumn().copy();
                column.setColumnIndex(i);
                columnList.add(column);
            } else {
                Column column = new Column(selectColumn.getSelectColumnName(), selectColumn.getColumnType(), i, -1);
                column.setAlias(selectColumn.getAlias());
                column.setTableAlias(selectColumn.getTableAlias());
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

    public void setColumnType(Byte columnType) {
        this.columnType = columnType;
    }

    public Byte getColumnType() {
        return columnType;
    }

    public void setFuncArgs(List<FuncArg> funcArgs) {
        this.funcArgs = funcArgs;
    }

    public List<FuncArg> getFuncArgs() {
        return funcArgs;
    }

    public void setColumnExpression(SelectColumnExpression columnExpression) {
        this.columnExpression = columnExpression;
    }

    public SelectColumnExpression getColumnExpression() {
        return columnExpression;
    }
}
