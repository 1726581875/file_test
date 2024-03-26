package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.util.SqlParserUtils;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.cursor.RowEntity;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
public class ConstantColumnExpr extends SelectColumnExpr {

    private Byte type;

    private Object value;

    public ConstantColumnExpr(Byte type, Object value) {
        this.type = type;
        this.value = value;
    }

    public ConstantColumnExpr(String value) {
        this.value = value;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        Column column = new Column(null, type, -1, -1);
        column.setValue(value);
        return column;
    }

    public static ConstantColumnExpr parseConstantValue(String value) {
        // 判断常量类型
        Byte constantType = getConstantType(value);

        if(constantType == DbTypeConstant.CHAR) {
            String valueStr = SqlParserUtils.getUnquotedStr(value);
            return new ConstantColumnExpr(constantType, valueStr);
        } else if(constantType == DbTypeConstant.INT_8) {
            return new ConstantColumnExpr(constantType, Long.valueOf(value));
        } else if(constantType == DbTypeConstant.DOUBLE) {
            return new ConstantColumnExpr(constantType, Double.parseDouble(value));
        }
        throw ExceptionUtil.buildDbException("解释常量失败，未知的常量类型:{}", value);
    }


    private static Byte getConstantType(String value) {
        if (SqlParserUtils.isContainQuotes(value)) {
            return DbTypeConstant.CHAR;
        } else if (isNumericString(value)) {
            if (value.contains(".")) {
                return DbTypeConstant.DOUBLE;
            } else {
                return DbTypeConstant.INT_8;
            }
        }
        throw ExceptionUtil.buildDbException("未知的常量类型:{}", value);
    }



    public static boolean isNumericString(String input) {
        try {
            // 使用Double类的parseDouble方法尝试将字符串转换为数字
            Double.parseDouble(input);
            // 转换成功，说明是数字字符串
            return true;
        } catch (NumberFormatException e) {
            // 转换出错，说明不是数字字符串
            return false;
        }
    }

    public Byte getType() {
        return type;
    }
}
