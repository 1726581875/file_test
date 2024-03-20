package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.function.FuncArg;
import com.moyu.xmz.store.common.dto.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/10/24
 */
public abstract class SelectColumnExpr extends Expression {

    @Override
    public Expression optimize() {
        return null;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }

    public static TableColumnExpr newSimpleTableColumnExpr(Column tableColumn) {
        return new TableColumnExpr(tableColumn);
    }

    public static FuncColumnExpr newFuncColumnExpr(String functionName, List<FuncArg> funcArgList) {
        return new FuncColumnExpr(functionName, funcArgList);
    }

    public static ConstantColumnExpr newConstantColumnExpr(String constantValue) {
        return new ConstantColumnExpr(constantValue);
    }

}
