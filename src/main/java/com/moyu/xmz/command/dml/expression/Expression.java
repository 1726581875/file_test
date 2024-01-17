package com.moyu.xmz.command.dml.expression;

import com.moyu.xmz.command.dml.sql.Query;
import com.moyu.xmz.command.dml.sql.QueryTable;
import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public abstract class Expression {

    public abstract Object getValue(RowEntity rowEntity);

    public static boolean isMatch(RowEntity row, Expression condition) {

        if(row.isDeleted()) {
            return false;
        }

        if (condition == null || (boolean) condition.getValue(row)) {
            return true;
        }
        return false;
    }

    public abstract Expression optimize();


    public void setSelectIndexes(Query query) {

    }

    public abstract void getSQL(StringBuilder sqlBuilder);

    public String getConditionSQL(){
        StringBuilder conditionSql = new StringBuilder();
        getSQL(conditionSql);
        return conditionSql.toString();
    }

    public Expression getJoinCondition(QueryTable mainTable, QueryTable joinTable){
        return null;
    }


}
