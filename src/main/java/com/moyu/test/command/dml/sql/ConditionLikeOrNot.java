package com.moyu.test.command.dml.sql;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionLikeOrNot extends AbstractCondition2 {

    private Column column;

    private String value;

    private boolean isLike;


    public ConditionLikeOrNot(Column column, String value, boolean isLike) {
        this.column = column;
        this.value = value;
        this.isLike = isLike;
    }

    @Override
    public boolean getResult(RowEntity row) {
        Column columnData = getColumnData(column, row);
        Object left = columnData.getValue();

        if(left == null) {
            return false;
        }

        String leftValue = (String) left;
        String sqlPattern = value;
        if (isLike) {
            return likeMatch(leftValue, sqlPattern);
        } else {
            return !likeMatch(leftValue, sqlPattern);
        }
    }

    private boolean likeMatch(String input, String sqlPattern){
        Pattern regex = Pattern.compile(sqlPattern.replace("%", ".*"));
        Matcher matcher = regex.matcher(input);
        return matcher.matches();
    }

}
