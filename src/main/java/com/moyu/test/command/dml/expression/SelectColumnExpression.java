package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.function.FuncArg;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.constant.FunctionConstant;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author xiaomingzhang
 * @date 2023/10/24
 */
public class SelectColumnExpression extends Expression {

    private Column tableColumn;

    private String functionName;

    private List<FuncArg> funcArgList;

    private String constantValue;

    public SelectColumnExpression(Column tableColumn) {
        this.tableColumn = tableColumn;
    }

    public SelectColumnExpression(String constantValue) {
        this.constantValue = constantValue;
    }

    public SelectColumnExpression(String functionName, List<FuncArg> funcArgList) {
        this.functionName = functionName;
        this.funcArgList = funcArgList;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        // 空select null;
        if (this.tableColumn == null && this.functionName == null && this.constantValue == null) {

            return null;
        }
        // 常量select 1;
        if (this.constantValue != null) {
            return this.constantValue;
        }
        // 表字段
        if (this.tableColumn != null) {
            Column column = rowEntity.getColumn(this.tableColumn.getColumnName(), this.tableColumn.getTableAlias());
            return column;
        }
        // 函数
        if (this.functionName != null) {
            switch (this.functionName) {
                case FunctionConstant.FUNC_UUID:
                    String uuid = UUID.randomUUID().toString();
                    Column uuidColumn = new Column(FunctionConstant.FUNC_UUID, ColumnTypeConstant.CHAR, -1, uuid.length());
                    uuidColumn.setValue(uuid);
                    return uuidColumn;
                case FunctionConstant.FUNC_NOW:
                    Date now = new Date();
                    Column dateColumn = new Column(FunctionConstant.FUNC_UUID, ColumnTypeConstant.TIMESTAMP, -1, 8);
                    dateColumn.setValue(now);
                    return dateColumn;
                default:
                    ExceptionUtil.throwSqlIllegalException("不支持函数{}", this.functionName);
            }
        }

        return null;
    }

    @Override
    public Expression optimize() {
        return null;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }
}
