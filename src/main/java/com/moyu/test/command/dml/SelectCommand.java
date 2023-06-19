package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.command.dml.function.*;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.constant.FunctionConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.cursor.*;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.SelectColumn;
import com.moyu.test.util.PathUtil;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class SelectCommand extends AbstractCommand {

    private Integer databaseId;
    /**
     * 查询结果
     */
    private QueryResult queryResult;
    
    private Query query;


    public SelectCommand(Integer databaseId, Query query) {
        this.databaseId = databaseId;
        this.query = query;

    }

    @Override
    public String execute() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        QueryResult queryResult = null;
/*        if((query.getMainTable().getJoinTables() == null || query.getMainTable().getJoinTables().size() == 0)
                && query.getMainTable().getSubQuery() == null) {
            queryResult = execQuery();
        } else {
            queryResult = subOrJoinQuery();
        }*/
        Cursor queryResultCursor = this.query.getQueryResultCursor();

        queryResult = parseQueryResult(queryResultCursor);
        long queryEndTime = System.currentTimeMillis();

        // 解析结果，打印拼接结果字符串
        return getResultPrintStr(queryResult, queryStartTime, queryEndTime);
    }


    private QueryResult parseQueryResult(Cursor cursor) {
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            result.addRow(row.getColumns());
        }
        return result;
    }


    public QueryResult execQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.query.getMainTable().getTableName());
            dataChunkStore = new DataChunkStore(fileFullPath);
            List<Column[]> dataList = null;
            Cursor cursor = query.getQueryCursor();
            if (useFunction()) {
                dataList = getFunctionResultList(cursor);
            } else if (useGroupBy()) {
                dataList = getGroupByResultList(cursor);
            } else {
                dataList = cursorQuery(cursor);
            }
            result.addAll(dataList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        this.queryResult = result;
        return this.queryResult;
    }


    public QueryResult subOrJoinQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());
        try {
            Cursor mainCursor = this.query.getQueryCursor();
            List<Column[]> dataList = lastFilter(mainCursor);
            result.addAll(dataList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.queryResult = result;
        return this.queryResult;
    }


    private List<Column[]> lastFilter(Cursor cursor) {
        List<Column[]> dataList = new ArrayList<>();
        int currIndex = 0;
        RowEntity mainRow = null;
        try {
            while ((mainRow = cursor.next()) != null) {
                if (this.query.isMatchLimit(this.query, currIndex) && ConditionComparator.isMatch(mainRow, query.getMainTable().getTableCondition())) {
                    Column[] columnData = mainRow.getColumns();
                    Column[] resultColumns = query.filterColumns(columnData, query.getSelectColumns());
                    dataList.add(resultColumns);
                }
                if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                    break;
                }
                currIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cursor.close();
        }
        return dataList;
    }



    /**
     * TODO当前只能做到SELECT后面全部是函数或者全部是字段
     * @return
     */
    private boolean useFunction() {
        // 如果第一个是函数，后面必须都是函数
        if (query.getSelectColumns()[0].getFunctionName() != null) {
            for (SelectColumn c : query.getSelectColumns()) {
                if (c.getFunctionName() == null) {
                    throw new SqlIllegalException("sql语法有误");
                }
            }
            return true;
        }
        // 否则就是查询
        return false;
    }

    private boolean useGroupBy() {
        if(this.query.getGroupByColumnName() != null) {
            if (query.getSelectColumns().length == 1) {
                return true;
            }
            for (int i = 1; i < query.getSelectColumns().length; i++) {
                SelectColumn c = query.getSelectColumns()[i];
                if (c.getFunctionName() == null) {
                    throw new SqlIllegalException("sql语法有误");
                }
            }
            return true;
        }
        return false;
    }


    private List<Column[]> cursorQuery(Cursor cursor) {
        List<Column[]> dataList = new ArrayList<>();
        int currIndex = 0;
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            boolean matchCondition = ConditionComparator.isMatch(row, this.query.getConditionTree());
            if (matchCondition && this.query.isMatchLimit(this.query, currIndex)) {
                Column[] columnData = row.getColumns();
                Column[] resultColumns = this.query.filterColumns(columnData, query.getSelectColumns());
                dataList.add(resultColumns);
            }
            if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                return dataList;
            }
            currIndex++;
        }
        return dataList;
    }






    private List<Column[]> getFunctionResultList(Cursor cursor) {

        List<Column[]> dataList = new ArrayList<>();
        // 1、初始化所有统计函数对象
        List<StatFunction> statFunctions = getFunctionList();
        // 2、遍历数据，执行计算函数
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            if (ConditionComparator.isMatch(row, this.query.getConditionTree())) {
                for (StatFunction statFunction : statFunctions) {
                    statFunction.stat(row.getColumns());
                }
            }
        }
        // 处理执行结果
        Column[] resultColumns = new Column[statFunctions.size()];
        for (int i = 0; i < statFunctions.size(); i++) {
            StatFunction statFunction = statFunctions.get(i);
            resultColumns[i] = functionResultToColumn(statFunction, i);
        }
        dataList.add(resultColumns);


        return dataList;
    }


    private Column functionResultToColumn(StatFunction statFunction, int columnIndex) {
        Long statResult = statFunction.getValue();
        Column resultColumn = null;
        Column c = query.getSelectColumns()[columnIndex].getColumn();
        // 日期类型
        if (!query.getSelectColumns()[columnIndex].getFunctionName().equals(FunctionConstant.FUNC_COUNT)
                && c != null && c.getColumnType() == DbColumnTypeConstant.TIMESTAMP) {
            resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.TIMESTAMP, columnIndex, 8);
            resultColumn.setValue(statResult == null ? null : new Date(statResult));
        } else {
            // 数字类型
            resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.INT_8, columnIndex, 8);
            resultColumn.setValue(statResult);
        }
        return resultColumn;
    }


    private List<Column[]> getGroupByResultList(Cursor cursor) {

        List<Column[]> dataList = new ArrayList<>();
        Map<Column, List<StatFunction>> groupByMap = new HashMap<>();

        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            if (ConditionComparator.isMatch(row, this.query.getConditionTree())) {
                Column column = getColumn(row.getColumns(), this.query.getGroupByColumnName());
                List<StatFunction> statFunctions = groupByMap.getOrDefault(column, getFunctionList());
                // 进入计算函数
                for (StatFunction statFunction : statFunctions) {
                    statFunction.stat(row.getColumns());
                }
                groupByMap.put(column, statFunctions);
            }
        }

        // 汇总执行结果
        for (Column groupByColumn : groupByMap.keySet()) {
            List<StatFunction> statFunctions = groupByMap.get(groupByColumn);
            Column[] resultColumns = new Column[statFunctions.size() + 1];
            // 第一列为group by字段
            resultColumns[0] = groupByColumn;
            // 其余字段为统计函数
            for (int i = 0; i < statFunctions.size(); i++) {
                int index = i + 1;
                StatFunction statFunction = statFunctions.get(i);
                resultColumns[index] = functionResultToColumn(statFunction, i);
            }
            dataList.add(resultColumns);
        }


        return dataList;
    }

    private Column getColumn(Column[] columns, String columnName) {
        for (Column c : columns) {
            if (c.getColumnName().equals(columnName)) {
                return c;
            }
        }
        return null;
    }



    private List<StatFunction> getFunctionList() {
        List<StatFunction> statFunctions = new ArrayList<>(query.getSelectColumns().length);
        for (SelectColumn selectColumn : query.getSelectColumns()) {

            String functionName = selectColumn.getFunctionName();
            if (functionName == null) {
               continue;
            }

            String columnName = selectColumn.getArgs()[0];
            if (columnName == null) {
                throw new SqlIllegalException("sql语法错误，column应当不为空");
            }
            switch (functionName) {
                case FunctionConstant.FUNC_COUNT:
                    statFunctions.add(new CountFunction(columnName));
                    break;
                case FunctionConstant.FUNC_SUM:
                    statFunctions.add(new SumFunction(columnName));
                    break;
                case FunctionConstant.FUNC_MIN:
                    statFunctions.add(new MinFunction(columnName));
                    break;
                case FunctionConstant.FUNC_MAX:
                    statFunctions.add(new MaxFunction(columnName));
                    break;
                default:
                    throw new SqlIllegalException("sql语法错误，不支持该函数：" + functionName);
            }
        }
        return statFunctions;
    }



    private void appendLine(StringBuilder stringBuilder) {
        stringBuilder.append(" ");
        for (SelectColumn column : query.getSelectColumns()) {
            int length = column.getSelectColumnName().length();
            while (length > 0) {
                stringBuilder.append("--");
                length--;
            }
        }
        stringBuilder.append("\n");
    }

    private String getStr(char c, int num) {
        StringBuilder stringBuilder = new StringBuilder();
        while (num > 0) {
            stringBuilder.append(c);
            num--;
        }
        return stringBuilder.toString();
    }


    private String getResultPrintStr(QueryResult queryResult, long queryStartTime,long queryEndTime) {
        // 解析结果，打印拼接结果字符串
        StringBuilder stringBuilder = new StringBuilder();
        // 分界线
        appendLine(stringBuilder);


        SelectColumn[] selectColumns = queryResult.getSelectColumns();
        // 表头
        String tableHeaderStr = "";
        for (SelectColumn column : query.getSelectColumns()) {
            String value = getColumnNameStr(column);
            tableHeaderStr = tableHeaderStr + " | " + value;
        }
        stringBuilder.append(tableHeaderStr + " | " + "\n");

        // 分界线
        appendLine(stringBuilder);

        SelectColumn[] resultColumns = queryResult.getSelectColumns();
        // 值
        List<Object[]> resultRows = queryResult.getResultRows();
        for (int i = 0; i < resultRows.size(); i++) {
            Object[] rowValues = resultRows.get(i);
            String rowStr = "";
            for (int j = 0; j < rowValues.length; j++) {
                Object value = rowValues[j];
                String valueStr = (value == null ? "" : valueToString(value));
                int length = getColumnNameStr(resultColumns[j]).length();
                if(length > valueStr.length()) {
                    int spaceNum = (length - valueStr.length()) / 2;
                    rowStr = rowStr + " | "+ getStr(' ',spaceNum) + valueStr + getStr(' ',spaceNum);
                } else {
                    rowStr = rowStr + " | " + valueStr;
                }
            }
            stringBuilder.append(rowStr + " | " + "\n");
        }

        stringBuilder.append("查询结果行数:" +  resultRows.size() + ", 耗时:" + (queryEndTime - queryStartTime)  + "ms");

        return stringBuilder.toString();
    }


    private String getColumnNameStr(SelectColumn column) {
        String value = column.getAlias() == null ? column.getSelectColumnName() : column.getAlias();
        if (column.getTableAlias() != null) {
            value = column.getTableAlias() + "." + value;
        }
        return value;
    }

    private String valueToString(Object value) {
        if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.format((Date) value);
        } else {
            return value.toString();
        }
    }



    public QueryResult getQueryResult() {
        return queryResult;
    }


    public Integer getLimit() {
        return query.getLimit();
    }


    public Integer getOffset() {
        return query.getOffset();
    }
    
}
