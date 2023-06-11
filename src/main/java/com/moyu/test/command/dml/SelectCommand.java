package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.command.dml.function.*;
import com.moyu.test.command.dml.plan.SelectIndex;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.constant.FunctionConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.cursor.*;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.SelectColumn;
import com.moyu.test.util.PathUtil;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class SelectCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;
    /**
     * 表所有的字段
     */
    private Column[] columns;

    private ConditionTree2 conditionTree2;

    /**
     * 查询计划
     */
    private SelectIndex selectPlan;

    private String groupByColumnName;
    /**
     * 查询结果
     */
    private QueryResult queryResult;


    private FromTable mainTable;
    
    private Query query;


    public SelectCommand(Integer databaseId, Query query) {
        this.databaseId = databaseId;
        this.mainTable = query.getMainTable();
        this.tableName = mainTable.getTableName();
        this.columns = mainTable.getAllColumns();
        this.query = query;
        this.groupByColumnName = query.getGroupByColumnName();
        this.selectPlan = query.getSelectIndex();
        this.conditionTree2 = query.getConditionTree();

    }

    @Override
    public String execute() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        QueryResult queryResult = null;
        if((mainTable.getJoinTables() == null || mainTable.getJoinTables().size() == 0)
                && mainTable.getSubQuery() == null) {
            queryResult = execQuery();
        } else if(mainTable.getSubQuery() != null) {
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
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
            List<Column[]> dataList = null;
            // select统计函数
            if (useFunction()) {
                dataList = getFunctionResultList(dataChunkNum, dataChunkStore);
            } else if (useGroupBy()) {
                dataList = getGroupByResultList(dataChunkNum, dataChunkStore);
            } else {
                if (selectPlan == null) {
                    System.out.println("不使用索引");
                    DefaultCursor cursor = new DefaultCursor(dataChunkStore, this.columns);
                    dataList = cursorQuery(cursor);
                } else {
                    System.out.println("使用索引查询，索引:" + selectPlan.getIndexName());
                    String indexPath = PathUtil.getIndexFilePath(this.databaseId, this.tableName, selectPlan.getIndexName());
                    IndexCursor cursor = new IndexCursor(dataChunkStore, columns, selectPlan.getIndexColumn(), indexPath);
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
        List<Column[]> dataList = new ArrayList<>();
        int currIndex = 0;

        Cursor mainCursor = null;
        try {
            Stack<Query> queryStack = new Stack<>();
            Query q = query;
            while ((q = q.getMainTable().getSubQuery()) != null){
                queryStack.add(q);
            }

            mainCursor = execSubQuery(queryStack);

            // 连表后的数据结果再按条件进行筛选
            RowEntity mainRow = null;
            while ((mainRow = mainCursor.next()) != null) {
                boolean matchCondition = ConditionComparator.isMatchRow(mainRow, query.getConditionTree());
                if (matchCondition && isMatchLimit(currIndex)) {
                    Column[] columnData = mainRow.getColumns();
                    Column[] resultColumns = filterColumns(columnData);
                    dataList.add(resultColumns);
                }
                if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                    break;
                }
                currIndex++;
            }

            result.addAll(dataList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mainCursor.close();
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
                mainCursor = subQueryTest(q);
            }
            RowEntity mainRow = null;
            int currIndex = 0;
            List<RowEntity> rowEntityList = new ArrayList<>();
            while ((mainRow = mainCursor.next()) != null) {
                boolean matchCondition = ConditionComparator.isMatchRow(mainRow, q.getConditionTree());
                if (matchCondition && isMatchLimit(currIndex)) {
                    Column[] columnData = mainRow.getColumns();
                    Column[] resultColumns = filterColumns(columnData);
                    rowEntityList.add(new RowEntity(resultColumns));
                }
                if (q.getLimit() != null && rowEntityList.size() >= q.getLimit()) {
                    break;
                }
                currIndex++;
            }

            mainCursor = new MemoryTemTableCursor(rowEntityList, mainCursor.getColumns());
        }

        return mainCursor;
    }



    private Cursor subQueryTest(Query query) {

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
        DataChunkStore mainTableStore = null;
        int currIndex = 0;
        try {
            mainTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.databaseId, mainTable.getTableName()));
            List<Column[]> dataList = new ArrayList<>();
            Cursor mainCursor = new DefaultCursor(mainTableStore, mainTable.getTableColumns());
            // join table
            List<FromTable> joinTables = mainTable.getJoinTables();
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

            // 连表后的数据结果再按条件进行筛选
            RowEntity mainRow = null;
            while ((mainRow = mainCursor.next()) != null) {
                boolean matchCondition = ConditionComparator.isMatchRow(mainRow, mainTable.getTableCondition());
                if (matchCondition && isMatchLimit(currIndex)) {
                    Column[] columnData = mainRow.getColumns();
                    Column[] resultColumns = filterColumns(columnData);
                    dataList.add(resultColumns);
                }
                if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                    break;
                }
                currIndex++;
            }

            result.addAll(dataList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mainTableStore.close();
        }

        this.queryResult = result;
        return this.queryResult;
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
        if(groupByColumnName != null) {
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
            boolean matchCondition = ConditionComparator.isMatchRow(row, conditionTree2);
            if (matchCondition && isMatchLimit(currIndex)) {
                Column[] columnData = row.getColumns();
                Column[] resultColumns = filterColumns(columnData);
                dataList.add(resultColumns);
            }
            if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                return dataList;
            }
            currIndex++;
        }
        return dataList;
    }




    private List<Column[]> getColumnDataListUseIndex(SelectIndex selectPlan, DataChunkStore dataChunkStore) {
        List<Column[]> dataList = new ArrayList<>();
        try {
            // 索引文件位置
            String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName +"_" + selectPlan.getIndexName() +".idx";

            Column indexColumn = selectPlan.getIndexColumn();
            // 根据索引拿到，数据块位置数组
            Long[] startPosArr = null;
            if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
                BpTreeMap<Integer, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Integer.class);
                startPosArr = bpTreeMap.get(Integer.valueOf((String) indexColumn.getValue()));
            } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()){
                BpTreeMap<Long, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Long.class);
                startPosArr = bpTreeMap.get(Long.valueOf((String) indexColumn.getValue()));
            } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
                BpTreeMap<String, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, String.class);
                startPosArr = bpTreeMap.get((String)indexColumn.getValue());
            }

            if (startPosArr == null || startPosArr.length == 0) {
                return new ArrayList<>();
            }

            // 遍历数据块，拿到符合条件的数据
            AtomicInteger currIndex = new AtomicInteger(0);
            for (Long starPos : startPosArr) {
                DataChunk dataChunk = dataChunkStore.getChunkByPos(starPos);
                analyzeDataChunkGetRowList(dataChunk, dataList, currIndex);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataList;
    }


    public void analyzeDataChunkGetRowList(DataChunk dataChunk, List<Column[]> dataList, AtomicInteger currIndex) {
        List<RowData> dataRowList = dataChunk.getDataRowList();
        for (int j = 0; j < dataRowList.size(); j++) {
            RowData rowData = dataRowList.get(j);
            Column[] columnData = rowData.getColumnData(columns);
            // 按照条件过滤
            if (conditionTree2 == null) {
                Column[] filterColumns = filterColumns(columnData);
                // 判断是否符合query.getLimit()、query.getOffset()
                if (isMatchLimit(currIndex.get())) {
                    dataList.add(filterColumns);
                }
                if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                    break;
                }
                currIndex.addAndGet(1);
            } else {
                boolean compareResult = ConditionComparator.isMatchRow(new RowEntity(columnData), conditionTree2);
                if (compareResult) {
                    Column[] filterColumns = filterColumns(columnData);
                    // 判断是否符合query.getLimit()、query.getOffset()
                    if (isMatchLimit(currIndex.get())) {
                        dataList.add(filterColumns);
                    }
                    if (query.getLimit() != null && dataList.size() >= query.getLimit()) {
                        break;
                    }
                    currIndex.addAndGet(1);
                }
            }
        }
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


    private List<Column[]> getFunctionResultList(int dataChunkNum, DataChunkStore dataChunkStore) {

        List<Column[]> dataList = new ArrayList<>();

        // 1、初始化所有统计函数对象
        List<StatFunction> statFunctions = getFunctionList();

        // 2、遍历数据，执行计算函数
        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            // 获取数据块包含的数据行
            List<RowData> dataRowList = chunk.getDataRowList();
            for (int j = 0; j < dataRowList.size(); j++) {
                RowData rowData = dataRowList.get(j);
                Column[] columnData = rowData.getColumnData(columns);
                // 没有where条件
                if(conditionTree2 == null) {
                    // 进入计算函数
                    for (StatFunction statFunction : statFunctions) {
                        statFunction.stat(columnData);
                    }
                    // 按where条件过滤
                } else {
                    boolean compareResult = ConditionComparator.isMatchRow(new RowEntity(columnData), conditionTree2);
                    if(compareResult) {
                        // 进入计算函数
                        for (StatFunction statFunction : statFunctions) {
                            statFunction.stat(columnData);
                        }
                    }
                }
            }
        }

        // 汇总执行结果
        Column[] resultColumns = new Column[statFunctions.size()];
        for (int i = 0; i < statFunctions.size(); i++) {
            StatFunction statFunction = statFunctions.get(i);

            Long statResult = statFunction.getValue();
            Column resultColumn = null;
            Column c = query.getSelectColumns()[i].getColumn();
            // 日期类型
            if(!query.getSelectColumns()[i].getFunctionName().equals(FunctionConstant.FUNC_COUNT)
                    && c != null && c.getColumnType() == DbColumnTypeConstant.TIMESTAMP) {
                resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.TIMESTAMP, i, 8);
                resultColumn.setValue(statResult == null ? null : new Date(statResult));
            } else {
                // 数字类型
                resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.INT_8, i, 8);
                resultColumn.setValue(statResult);
            }

            resultColumns[i] = resultColumn;
        }
        dataList.add(resultColumns);


        return dataList;
    }


    private List<Column[]> getGroupByResultList(int dataChunkNum, DataChunkStore dataChunkStore) {

        List<Column[]> dataList = new ArrayList<>();
        Map<Column, List<StatFunction>> groupByMap = new HashMap<>();

        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            // 获取数据块包含的数据行
            List<RowData> dataRowList = chunk.getDataRowList();
            for (int j = 0; j < dataRowList.size(); j++) {
                RowData rowData = dataRowList.get(j);
                Column[] columnData = rowData.getColumnData(columns);
                // 没有where条件
                if(conditionTree2 == null) {
                    Column column = getColumn(columnData, this.groupByColumnName);
                    List<StatFunction> statFunctions = groupByMap.getOrDefault(column, getFunctionList());
                    // 进入计算函数
                    for (StatFunction statFunction : statFunctions) {
                        statFunction.stat(columnData);
                    }
                    groupByMap.put(column, statFunctions);
                } else {
                    boolean compareResult = ConditionComparator.isMatchRow(new RowEntity(columnData), conditionTree2);
                    if(compareResult) {
                        Column column = getColumn(columnData, this.groupByColumnName);
                        List<StatFunction> statFunctions = groupByMap.getOrDefault(column, getFunctionList());
                        // 进入计算函数
                        for (StatFunction statFunction : statFunctions) {
                            statFunction.stat(columnData);
                        }
                        groupByMap.put(column, statFunctions);
                    }
                }
            }
        }

        // 汇总执行结果
        for (Column groupByColumn : groupByMap.keySet()) {
            List<StatFunction> statFunctions = groupByMap.get(groupByColumn);
            Column[] resultColumns = new Column[statFunctions.size() + 1];
            resultColumns[0] = groupByColumn;
            for (int i = 0; i < statFunctions.size(); i++) {
                int index = i + 1;
                StatFunction statFunction = statFunctions.get(i);
                Long statResult = statFunction.getValue();
                Column resultColumn = null;
                Column c = query.getSelectColumns()[index].getColumn();
                // 日期类型
                if (!query.getSelectColumns()[index].getFunctionName().equals(FunctionConstant.FUNC_COUNT)
                        && c != null && c.getColumnType() == DbColumnTypeConstant.TIMESTAMP) {
                    resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.TIMESTAMP, index, 8);
                    resultColumn.setValue(statResult == null ? null : new Date(statResult));
                } else {
                    // 数字类型
                    resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.INT_8, index, 8);
                    resultColumn.setValue(statResult);
                }
                resultColumns[index] = resultColumn;
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



    private Column[] filterColumns(Column[] columnData) {
        Column[] resultColumns = new Column[query.getSelectColumns().length];
        for (int i = 0; i < query.getSelectColumns().length; i++) {
            SelectColumn selectColumn = query.getSelectColumns()[i];
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


    public SelectIndex getSelectPlan() {
        return selectPlan;
    }
    public void setSelectPlan(SelectIndex selectPlan) {
        this.selectPlan = selectPlan;
    }

    public String getGroupByColumnName() {
        return groupByColumnName;
    }

    public void setGroupByColumnName(String groupByColumnName) {
        this.groupByColumnName = groupByColumnName;
    }


    public void setMainTable(FromTable mainTable) {
        this.mainTable = mainTable;
    }

    public void setConditionTree2(ConditionTree2 conditionTree2) {
        this.conditionTree2 = conditionTree2;
    }
}
