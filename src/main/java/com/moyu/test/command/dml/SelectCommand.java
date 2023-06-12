package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.command.dml.function.*;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.constant.FunctionConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.exception.SqlExecutionException;
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
        if((query.getMainTable().getJoinTables() == null || query.getMainTable().getJoinTables().size() == 0)
                && query.getMainTable().getSubQuery() == null) {
            queryResult = execQuery();
        } else if(query.getMainTable().getSubQuery() != null) {
            queryResult = subQuery();
        } else {
            queryResult = joinQuery();
        }
        long queryEndTime = System.currentTimeMillis();

        // 解析结果，打印拼接结果字符串
        return getResultPrintStr(queryResult, queryStartTime, queryEndTime);
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
            // select统计函数
            if (useFunction()) {
                DefaultCursor cursor = new DefaultCursor(dataChunkStore, this.query.getMainTable().getAllColumns());
                dataList = getFunctionResultList(cursor);
            } else if (useGroupBy()) {
                DefaultCursor cursor = new DefaultCursor(dataChunkStore, this.query.getMainTable().getAllColumns());
                dataList = getGroupByResultList(cursor);
            } else {
                if (this.query.getSelectIndex() == null) {
                    System.out.println("不使用索引");
                    DefaultCursor cursor = new DefaultCursor(dataChunkStore, this.query.getMainTable().getAllColumns());
                    dataList = cursorQuery(cursor);
                } else {
                    System.out.println("使用索引查询，索引:" + this.query.getSelectIndex().getIndexName());
                    String indexPath = PathUtil.getIndexFilePath(this.databaseId, this.query.getMainTable().getTableName(), this.query.getSelectIndex().getIndexName());
                    IndexCursor cursor = new IndexCursor(dataChunkStore, this.query.getMainTable().getAllColumns(), this.query.getSelectIndex().getIndexColumn(), indexPath);
                    dataList = cursorQuery(cursor);
                }
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


    public QueryResult subQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());
        try {
            Stack<Query> queryStack = new Stack<>();
            Query q = query;
            queryStack.add(q);
            while ((q = q.getMainTable().getSubQuery()) != null) {
                queryStack.add(q);
            }

            Cursor mainCursor = execSubQuery(queryStack);

            // 连表后的数据结果再按条件进行筛选
            List<Column[]> dataList = lastFilter(mainCursor);
            result.addAll(dataList);

            result.addAll(dataList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.queryResult = result;
        return this.queryResult;
    }


    private Cursor execSubQuery(Stack<Query> queryStack) {
        Cursor mainCursor = null;

        while (!queryStack.isEmpty()) {
            Query q = queryStack.pop();
            FromTable fromTable = q.getMainTable();
            if(fromTable.getSubQuery() == null) {
                mainCursor = getQueryCursor(q);
            }

            int currIndex = 0;
            RowEntity mainRow = null;
            String tableAlias = q.getMainTable().getAlias();
            List<RowEntity> rowEntityList = new ArrayList<>();
            while ((mainRow = mainCursor.next()) != null) {
                RowEntity rowEntity = new RowEntity(mainRow.getColumns(), tableAlias);
                if (ConditionComparator.isMatch(rowEntity, q.getConditionTree()) && isMatchLimit(currIndex)) {
                    Column[] columns = filterColumns(mainRow.getColumns(), q.getSelectColumns());
                    rowEntityList.add(new RowEntity(columns, tableAlias));
                }
                if (q.getLimit() != null && rowEntityList.size() >= q.getLimit()) {
                    break;
                }
                currIndex++;
            }

            Column[] columns = mainCursor.getColumns();
            Column.setColumnAlias(columns, tableAlias);
            mainCursor = new MemoryTemTableCursor(rowEntityList, columns);
        }

        return mainCursor;
    }



    private Cursor getQueryCursor(Query query) {

        DataChunkStore mainTableStore = null;
        Cursor mainCursor = null;
        try {
            FromTable mTable = query.getMainTable();
            mainTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.databaseId, mTable.getTableName()));
            mainCursor = new DefaultCursor(mainTableStore, mTable.getTableColumns());

            List<FromTable> joinTables = mTable.getJoinTables();
            // join table
            if(joinTables != null && joinTables.size() > 0) {
                for (FromTable joinTable : joinTables) {
                    DataChunkStore joinTableStore = null;
                    try {
                        joinTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.databaseId, joinTable.getTableName()));
                        DefaultCursor joinCursor = new DefaultCursor(joinTableStore, joinTable.getTableColumns());
                        // 进行连表操作
                        mainCursor = joinTable(mainCursor, joinCursor, joinTable.getJoinCondition(), joinTable.getJoinInType());
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        joinTableStore.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           // mainTableStore.close();
        }
        return mainCursor;
    }


    public QueryResult joinQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(query.getSelectColumns());
        result.setResultRows(new ArrayList<>());
        try {
            Cursor mainCursor = getQueryCursor(this.query);
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
                if (isMatchLimit(currIndex) && ConditionComparator.isMatch(mainRow, query.getMainTable().getTableCondition())) {
                    Column[] columnData = mainRow.getColumns();
                    Column[] resultColumns = filterColumns(columnData, query.getSelectColumns());
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


    private Cursor joinTable(Cursor leftCursor, Cursor rightCursor, ConditionTree2 joinCondition, String joinType) {

        // 字段元数据
        Column[] columns = Column.mergeColumns(leftCursor.getColumns(), rightCursor.getColumns());

        List<RowEntity> resultList = new ArrayList<>();
        // 内连接、左连接
        if (CommonConstant.JOIN_TYPE_INNER.equals(joinType)
                || CommonConstant.JOIN_TYPE_LEFT.equals(joinType)) {
            RowEntity leftRow = null;
            while ((leftRow = leftCursor.next()) != null) {
                List<RowEntity> rows = new ArrayList<>();
                RowEntity rightRow = null;
                while ((rightRow = rightCursor.next()) != null) {
                    if (isMatchJoinCondition(leftRow, rightRow, joinCondition)) {
                        RowEntity rowEntity = RowEntity.mergeRow(leftRow, rightRow);
                        rows.add(rowEntity);
                    }
                }
                // 左连接，如果右表没有匹配行，要加一个空行
                if (CommonConstant.JOIN_TYPE_LEFT.equals(joinType) && rows.size() == 0) {
                    RowEntity rightNullRow = new RowEntity(rightCursor.getColumns());
                    RowEntity rowEntity = RowEntity.mergeRow(leftRow, rightNullRow);
                    rows.add(rowEntity);
                }

                rightCursor.reset();
                resultList.addAll(rows);
            }
            return new MemoryTemTableCursor(resultList, columns);
            // 右连接
        } else if (CommonConstant.JOIN_TYPE_RIGHT.equals(joinType)) {
            RowEntity rightRow = null;
            while ((rightRow = rightCursor.next()) != null) {
                RowEntity leftRow = null;
                List<RowEntity> rows = new ArrayList<>();
                while ((leftRow = leftCursor.next()) != null) {
                    if (isMatchJoinCondition(leftRow, rightRow, joinCondition)) {
                        RowEntity rowEntity = RowEntity.mergeRow(leftRow, rightRow);
                        rows.add(rowEntity);
                    }
                }
                // 右连接，左表没有匹配行，加一个空行
                if (rows.size() == 0) {
                    RowEntity leftNullRow = new RowEntity(leftCursor.getColumns());
                    RowEntity rowEntity = RowEntity.mergeRow(leftNullRow, rightRow);
                    rows.add(rowEntity);
                }
                resultList.addAll(rows);
                leftCursor.reset();
            }
            return new MemoryTemTableCursor(resultList, columns);
        } else {
            throw new SqlIllegalException("不支持连接类型:" + joinType);
        }
    }




    private boolean isMatchJoinCondition(RowEntity leftRow, RowEntity rightRow, ConditionTree2 joinCondition) {

        Condition2 condition = joinCondition.getCondition();
        if(condition instanceof ConditionLeftRight) {
            ConditionLeftRight leftRight = (ConditionLeftRight) condition;
            // 左表字段
            Column leftColumn = leftRow.getColumn(leftRight.getLeft().getColumnName(), leftRight.getLeft().getTableAlias());
            // 右边字段
            Column rightColumn = rightRow.getColumn(leftRight.getRight().getColumnName(), leftRight.getRight().getTableAlias());

            return leftColumn.getValue() != null && leftColumn.getValue().equals(rightColumn.getValue());
        } else {
            throw new DbException("不支持连接条件");
        }
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
            if (matchCondition && isMatchLimit(currIndex)) {
                Column[] columnData = row.getColumns();
                Column[] resultColumns = filterColumns(columnData, query.getSelectColumns());
                dataList.add(resultColumns);
            }
            if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                return dataList;
            }
            currIndex++;
        }
        return dataList;
    }




    /**
     * 是否是符合query.getOffset()和query.getLimit()条件的行
     * @param currIndex
     * @return
     */
    private boolean isMatchLimit(int currIndex) {
        if (query.getOffset() != null && query.getLimit() != null) {
            int beginIndex = query.getOffset();
            int endIndex = query.getOffset() + query.getLimit() - 1;
            if (currIndex < beginIndex || currIndex > endIndex) {
                return false;
            }
        }
        return true;
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


    private RowEntity filterColumns(RowEntity row, SelectColumn[] selectColumns) {
        Column[] columns = filterColumns(row.getColumns(), selectColumns);
        return new RowEntity(columns);
    }


    private Column[] filterColumns(Column[] columnData, SelectColumn[] selectColumns) {
        Column[] resultColumns = new Column[selectColumns.length];
        for (int i = 0; i < selectColumns.length; i++) {
            SelectColumn selectColumn = selectColumns[i];
            for (Column c : columnData) {
                if(selectColumn.getTableAliasColumnName().equals(c.getTableAliasColumnName())) {
                    resultColumns[i] = c;
                }
            }
            if(resultColumns[i] == null) {
                throw new SqlExecutionException("字段不存在:" + selectColumn.getTableAliasColumnName());
            }
        }
        return resultColumns;
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
