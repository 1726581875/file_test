package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.constant.FuncConstant;
import com.moyu.xmz.common.util.SqlParserUtils;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
public class ConstantColumnExpr extends SelectColumnExpr {

    private String value;

    public ConstantColumnExpr(String value) {
        this.value = value;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        if (SqlParserUtils.isContainQuotes(this.value)) {
            // 字符串
            String valueStr = SqlParserUtils.getUnquotedStr(this.value);
            Column dateColumn = new Column(valueStr, DbTypeConstant.CHAR, -1, valueStr.length());
            dateColumn.setValue(valueStr);
            return dateColumn;
        } else if(isNumericString(this.value)) {
            // 浮点数
            Column column = null;
            if (value.contains(".")) {
                column = new Column(this.value, DbTypeConstant.DOUBLE, -1,1);
                column.setValue(Double.parseDouble(this.value));
            } else {
                Long valueLong = Long.valueOf(this.value);
                // 整数
                column = new Column(this.value, DbTypeConstant.INT_8, -1,8);
                column.setValue(valueLong);
            }
            return column;
        }
        return null;
    }



    public boolean isNumericString(String input) {
        try {
            Double.parseDouble(input); // 使用Double类的parseDouble方法尝试将字符串转换为数字
            return true; // 转换成功，说明是数字字符串
        } catch (NumberFormatException e) {
            return false; // 转换出错，说明不是数字字符串
        }
    }
}
