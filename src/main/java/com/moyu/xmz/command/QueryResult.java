package com.moyu.xmz.command;

import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.net.model.terminal.QueryResultDto;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.terminal.util.PrintResultUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class QueryResult {

    private SelectColumn[] selectColumns;

    private List<Object[]> resultRows = new ArrayList<>();

    /**
     * 描述
     */
    private String desc;
    /**
     * 数据量太大时候不一次传输，改为分批传输
     * 标记查询结果是否还有下一页数据，每页大小见CommonConfig.NET_TRAN_MAX_SIZE
     */
    private byte hasNext;


    public void addAll(List<Column[]> columnValueList) {
        for (Column[] columns : columnValueList) {
            addRow(columns);
        }
    }

    public void addRow(Column[] columnValues) {
        Object[] values = new Object[columnValues.length];
        for (int i = 0; i < columnValues.length; i++) {
            values[i] = columnValues[i].getValue();
        }
        addRow(values);
    }

    public void addRow(Object[] resultRow) {
        if (resultRows == null) {
            resultRows = new ArrayList<>();
        }
        resultRows.add(resultRow);
    }

    public static QueryResult simpleResult(Object resultValue) {
        SelectColumn nameColumn = SelectColumn.newColumn("执行结果", ColumnTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{nameColumn});
        queryResult.addRow(new Object[]{resultValue});
        return queryResult;
    }

    public List<Object[]> getResultRows() {
        return resultRows;
    }

    public void setResultRows(List<Object[]> resultRows) {
        this.resultRows = resultRows;
    }

    public SelectColumn[] getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(SelectColumn[] selectColumns) {
        this.selectColumns = selectColumns;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public byte getHasNext() {
        return hasNext;
    }

    public void setHasNext(byte hasNext) {
        this.hasNext = hasNext;
    }

    @Override
    public String toString() {
        return PrintResultUtil.getFormatResult(QueryResultDto.valueOf(this), (byte) 0, 100);
    }
}
