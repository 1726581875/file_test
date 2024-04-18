package com.moyu.xmz.command.dml.expression.column;

import com.moyu.xmz.command.dml.function.FuncArg;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.constant.FuncConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.cursor.RowEntity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author xiaomingzhang
 * @date 2024/3/20
 */
public class FuncColumnExpr extends SelectColumnExpr {
    /**
     * 函数名
     */
    private String functionName;
    /**
     * 函数参数
     */
    private List<FuncArg> funcArgList;
    /**
     * 函数返回结果类型
     * @See com.moyu.xmz.common.constant.DbTypeConstant
     */
    private Byte resultType;

    public FuncColumnExpr(String functionName, List<FuncArg> funcArgList) {
        this.functionName = functionName;
        this.funcArgList = funcArgList;
    }

    public FuncColumnExpr(String functionName, List<FuncArg> funcArgList, Byte resultType) {
        this.functionName = functionName;
        this.funcArgList = funcArgList;
        this.resultType = resultType;
    }


    @Override
    public Object getValue(RowEntity rowEntity) {
        switch (this.functionName) {
            case FuncConstant.FUNC_UUID:
                String uuid = UUID.randomUUID().toString();
                Column uuidColumn = new Column(FuncConstant.FUNC_UUID, DbTypeConstant.CHAR, -1, uuid.length());
                uuidColumn.setValue(uuid);
                return uuidColumn;
            case FuncConstant.FUNC_NOW:
                Date now = new Date();
                Column dateColumn = new Column(FuncConstant.FUNC_UUID, DbTypeConstant.TIMESTAMP, -1, 8);
                dateColumn.setValue(now);
                return dateColumn;
            case FuncConstant.NOW_TIMESTAMP:
                Column nowTimestamp = new Column(this.functionName, DbTypeConstant.INT_8, -1, 8);
                nowTimestamp.setValue(System.currentTimeMillis());
                return nowTimestamp;
            case FuncConstant.UNIX_TIMESTAMP:
            case FuncConstant.TO_TIMESTAMP:
                dateColumn = new Column(this.functionName, DbTypeConstant.INT_8, -1, 8);
                FuncArg funcArg = funcArgList.get(0);
                Object argValue = funcArg.getArgValue();
                if(FuncArg.CONSTANT.equals(funcArg.getArgType())) {
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date time = null;
                    String timeStr = (String) argValue;
                    try {
                        time = dateFormat.parse(timeStr);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        ExceptionUtil.throwDbException("时间格式转换发生异常,时间格式不合法{}", argValue);
                    }
                    dateColumn.setValue(time.getTime() / 1000);
                } else if(FuncArg.FUNCTION.equals(funcArg.getArgType())) {
                    FuncColumnExpr argFunc = (FuncColumnExpr) argValue;
                    Column argFuncValue = (Column) argFunc.getValue(rowEntity);
                    Long finalValue = null;
                    if(argFuncValue.getColumnType() == DbTypeConstant.TIMESTAMP) {
                        finalValue = ((Date) argFuncValue.getValue()).getTime() / 1000;
                    } else {
                        throw ExceptionUtil.buildDbException("函数参数参数不合法，函数{}不支持{}类型的参数",this.functionName, argFuncValue.getColumnType());
                    }
                    dateColumn.setValue(finalValue);
                } else {
                    throw ExceptionUtil.buildDbException("函数参数类型不合法{}", funcArg.getArgType());
                }
                return dateColumn;
            case FuncConstant.FROM_UNIXTIME:
                dateColumn = new Column(FuncConstant.FROM_UNIXTIME, DbTypeConstant.CHAR, -1, 8);
                Object timestampValue = funcArgList.get(0).getArgValue();
                DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Long timestamp = Long.valueOf((String) timestampValue);
                String timeResult= simpleDateFormat.format(new Date(timestamp * 1000));
                dateColumn.setValue(timeResult);
                return dateColumn;
            default:
                ExceptionUtil.throwSqlIllegalException("不支持函数{}", this.functionName);
        }
        return null;
    }


    public Byte getResultType() {
        return resultType;
    }
}
