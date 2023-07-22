package com.moyu.test.command.dml.expression;

import com.moyu.test.command.dml.sql.Query;
import com.moyu.test.store.data.cursor.RowEntity;

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

        // 左右条件相等,可以合并为一个；像(a=1 and b=1)
        if (l.equals(r)) {
            if (l instanceof ConstantValue) {
                boolean b = (Boolean) l.getValue(null);
                return new ConstantValue(b);
            }
            return l;
        }

        // 优化自矛盾的条件，如(a=1 and a=2)
        if(isAnd() && l instanceof SingleComparison
                && r instanceof SingleComparison) {
            SingleComparison lComparison = (SingleComparison) l;
            SingleComparison rComparison = (SingleComparison) r;
            if(lComparison.getLeft().equals(rComparison.getLeft())
                    && !lComparison.getRight().equals(rComparison.getRight())) {
                return new ConstantValue(false);
            }
        }


        // 优化常量表达式;
        if(isAnd()) {
            // 如(1=1 and 2=2)
            if(l instanceof ConstantValue && r instanceof ConstantValue) {
                boolean a = (Boolean)l.getValue(null);
                boolean b = (Boolean)r.getValue(null);
                return new ConstantValue(a && b);
            }

            // 如(1=1 and a=1)
            if(l instanceof ConstantValue) {
                boolean b = (Boolean)l.getValue(null);
                if(b == false) {
                    return new ConstantValue(false);
                } else {
                    return r;
                }
            }
            // 如(a=1 and 1=1)
            if(r instanceof ConstantValue) {
                boolean b = (Boolean)r.getValue(null);
                if(b == false) {
                    return new ConstantValue(false);
                } else {
                    return l;
                }
            }

        } else {
            // 如(1=1 or a=1)
            if(l instanceof ConstantValue) {
                boolean b = (Boolean)l.getValue(null);
                if(b == true) {
                    return new ConstantValue(true);
                }
            }
            // 如(a=1 or 1=1)
            if(r instanceof ConstantValue) {
                boolean b = (Boolean)r.getValue(null);
                if(b == true) {
                    return new ConstantValue(true);
                }
            }
            // 如(1=1 or 1=1)
            if(l instanceof ConstantValue && r instanceof ConstantValue) {
                boolean a = (Boolean)l.getValue(null);
                boolean b = (Boolean)r.getValue(null);
                return new ConstantValue(a || b);
            }
        }

        left = l;
        right = r;
        return this;
    }


    private boolean isAnd() {
        return TYPE_AND.equals(type);
    }


    @Override
    public void setSelectIndexes(Query query) {
        if(TYPE_AND.equals(type)) {
            left.setSelectIndexes(query);
            right.setSelectIndexes(query);
        }
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {
        sqlBuilder.append("(");
        left.getSQL(sqlBuilder);
        sqlBuilder.append(' ' + type + ' ');
        right.getSQL(sqlBuilder);
        sqlBuilder.append(")");
    }
}
