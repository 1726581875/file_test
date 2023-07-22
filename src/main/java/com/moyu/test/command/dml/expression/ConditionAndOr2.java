package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.session.LocalSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.type.Value;

/**
 * @author xiaomingzhang
 * @date 2023/7/17
 */
public class ConditionAndOr2 extends AbstractCondition {

    public final static String TYPE_AND = "AND";

    public final static String TYPE_OR = "OR";

    private String type;

    private Expression left;

    private Expression right;


    public ConditionAndOr2(String type, Expression left, Expression right) {
        this.type = type;
        this.left = left;
        this.right = right;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        boolean l = (Boolean) left.getValue(rowEntity);
        boolean r = (Boolean) right.getValue(rowEntity);
        if (TYPE_AND.equals(type)) {
            return l && r;
        } else {
            return l || r;
        }
    }


    @Override
    public Expression optimize() {

        Expression l = left.optimize();
        Expression r = right.optimize();


        if(TYPE_AND.equals(type)) {
            if(l instanceof ConstantValue) {
                boolean b = (Boolean)l.getValue(null);
                if(b == false) {
                    return new ConstantValue(false);
                }
            }

            if(r instanceof ConstantValue) {
                boolean b = (Boolean)r.getValue(null);
                if(b == false) {
                    return new ConstantValue(false);
                }
            }

            if(l instanceof ConstantValue && r instanceof ConstantValue) {
                boolean a = (Boolean)l.getValue(null);
                boolean b = (Boolean)r.getValue(null);
                return new ConstantValue(a && b);
            }

        } else {
            if(l instanceof ConstantValue) {
                boolean b = (Boolean)l.getValue(null);
                if(b == true) {
                    return new ConstantValue(true);
                }
            }
            if(r instanceof ConstantValue) {
                boolean b = (Boolean)r.getValue(null);
                if(b == true) {
                    return new ConstantValue(true);
                }
            }

            if(l instanceof ConstantValue && r instanceof ConstantValue) {
                boolean a = (Boolean)l.getValue(null);
                boolean b = (Boolean)r.getValue(null);
                return new ConstantValue(a || b);
            }
        }


        return this;
    }


    @Override
    public void setSelectIndexes(Query query) {
        if(TYPE_AND.equals(type)) {
            left.setSelectIndexes(query);
            right.setSelectIndexes(query);
        }
    }
}
