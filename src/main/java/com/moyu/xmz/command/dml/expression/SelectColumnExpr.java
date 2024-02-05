package com.moyu.xmz.command.dml.expression;

import com.moyu.xmz.command.dml.function.FuncArg;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.constant.FuncConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.common.dto.Column;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author xiaomingzhang
 * @date 2023/10/24
 */
public class SelectColumnExpr extends Expression {

    public static final String SIMPLE_COLUMN = "SIMPLE_COLUMN";
    public static final String FUNC_COLUMN = "FUNC_COLUMN";
    public static final String CONSTANT = "CONSTANT";
    public static final String NULL = "NULL";

    /**
     * SIMPLE_COLUMN
     */
    private String type;

    private Column tableColumn;

    private String functionName;

    private List<FuncArg> funcArgList;

    private String constantValue;

    public SelectColumnExpr(Column tableColumn, String type) {
        this.tableColumn = tableColumn;
        this.type = type;
    }

    public SelectColumnExpr(String constantValue) {
        this.constantValue = constantValue;
    }

    public SelectColumnExpr(String functionName, List<FuncArg> funcArgList) {
        this.functionName = functionName;
        this.funcArgList = funcArgList;
        this.type = FUNC_COLUMN;
    }

    @Override
    public Object getValue(RowEntity rowEntity) {
        switch (type) {
            case CONSTANT:
                // TODO 封装成字段形式
                return constantValue;
            case NULL:
                // TODO 封装成字段形式
                return null;
            case SIMPLE_COLUMN:
                Column column = rowEntity.getColumn(this.tableColumn.getColumnName(), this.tableColumn.getTableAlias());
                if(column == null) {
                    ExceptionUtil.throwSqlIllegalException("字段不存在:{}.{}",this.tableColumn.getTableAlias(), this.tableColumn.getColumnName());
                }
                return column;
            case FUNC_COLUMN:
                return getFunctionResult();
            default:
                ExceptionUtil.throwDbException("不支持该字段类型:{}", type);
        }
        return null;
    }


    private Column getFunctionResult() {

        Column resultColumn = null;
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
            case FuncConstant.UNIX_TIMESTAMP:
                dateColumn = new Column(FuncConstant.UNIX_TIMESTAMP, DbTypeConstant.INT_8, -1, 8);
                FuncArg funcArg = funcArgList.get(0);
                Object argValue = funcArg.getArgValue();
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date time = null;
                String timeStr = (String) argValue;
                try {
                    time = dateFormat.parse(timeStr);
                } catch(ParseException e) {
                    e.printStackTrace();
                    ExceptionUtil.throwDbException("时间格式转换发生异常,时间格式不合法{}", argValue);
                }
                dateColumn.setValue(time.getTime() / 1000);
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



    @Override
    public Expression optimize() {
        return null;
    }

    @Override
    public void getSQL(StringBuilder sqlBuilder) {

    }


    public static SelectColumnExpr newSimpleTableColumnExpr(Column tableColumn) {
        return new SelectColumnExpr(tableColumn, SIMPLE_COLUMN);
    }

    public static SelectColumnExpr newFuncColumnExpr(String functionName, List<FuncArg> funcArgList) {
        return new SelectColumnExpr(functionName, funcArgList);
    }

}
