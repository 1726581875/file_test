package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.dml.condition.Condition;
import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.condition.ConditionTree;
import com.moyu.test.command.dml.condition.TableOperation;
import com.moyu.test.command.dml.function.*;
import com.moyu.test.command.dml.plan.SelectPlan;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.constant.FunctionConstant;
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
    /**
     * 要查询的列/字段
     */
    private SelectColumn[] selectColumns;
    /**
     * 条件树
     */
    private ConditionTree conditionTree;
    /**
     * 查询计划
     */
    private SelectPlan selectPlan;

    private String groupByColumnName;

    private Integer limit;

    private Integer offset = 0;
    /**
     * 查询结果
     */
    private QueryResult queryResult;


    private TableOperation mainTable;


    public SelectCommand(Integer databaseId,
                         String tableName,
                         Column[] columns,
                         SelectColumn[] selectColumns) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.columns = columns;
        this.selectColumns = selectColumns;
    }

    @Override
    public String execute() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        QueryResult queryResult = null;
        if(mainTable.getJoinTables() == null || mainTable.getJoinTables().size() == 0) {
            queryResult = execQuery();
        } else {
            queryResult = joinQuery();
        }
        long queryEndTime = System.currentTimeMillis();

        // 解析结果，打印拼接结果字符串
        return getResultPrintStr(queryResult, queryStartTime, queryEndTime);
    }


    public QueryResult execQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(selectColumns);
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


    public QueryResult joinQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(selectColumns);
        result.setResultRows(new ArrayList<>());
        DataChunkStore mainTableStore = null;
        DataChunkStore joinTableStore = null;

        List<TableOperation> joinTables = mainTable.getJoinTables();
        int currIndex = 0;
        try {
            mainTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.databaseId, mainTable.getTableName()));
            List<Column[]> dataList = new ArrayList<>();
            Cursor mainCursor = new DefaultCursor(mainTableStore, mainTable.getAllColumns());
            // join table
            for (TableOperation joinTable : joinTables) {
                joinTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.databaseId, joinTable.getTableName()));
                DefaultCursor joinCursor = new DefaultCursor(joinTableStore, joinTable.getAllColumns());
                mainCursor = joinTable(mainCursor, joinCursor, joinTable.getJoinCondition());
            }

            RowEntity mainRow = null;
            while ((mainRow = mainCursor.next()) != null) {
                Column[] columnData = mainRow.getColumns();
                ConditionTree tableCondition = mainTable.getTableCondition();
                boolean matchCondition = tableCondition == null ? true : ConditionComparator.analyzeConditionTree(tableCondition, columnData);
                if (matchCondition && isMatchLimit(currIndex)) {
                    Column[] resultColumns = filterColumns(columnData);
                    dataList.add(resultColumns);
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


    private Cursor joinTable(Cursor mainCursor, Cursor joinCursor, ConditionTree joinCondition) {

        List<RowEntity> resultList = new ArrayList<>();
        RowEntity mainRow = null;
        while ((mainRow = mainCursor.next()) != null) {
            Column[] mainColumnData = mainRow.getColumns();
            RowEntity joinRow = null;
            while ((joinRow = joinCursor.next()) != null) {
                Column[] joinColumnData = joinRow.getColumns();
                if (matchJoinCondition(mainRow, joinRow, joinCondition)) {
                    Column[] allColumn = Column.mergeColumns(mainColumnData, joinColumnData);
                    resultList.add(new RowEntity(allColumn));
                }

            }
            joinCursor.reset();
        }

        return new MemoryTemTableCursor(resultList);
    }




    private boolean matchJoinCondition(RowEntity mainRow,  RowEntity joinRow, ConditionTree joinCondition) {
        Condition condition = joinCondition.getCondition();

        Column[] mainColumnData = mainRow.getColumns();
        String key1 = condition.getKey();
        String[] key1Split = key1.split("\\.");
        Object value1 = null;
        for (Column c : mainColumnData) {
            if(c.getColumnName().equals(key1Split[1])) {
                value1 = c.getValue();
            }
        }

        Column[] joinColumnData = joinRow.getColumns();
        String key2 = condition.getValue().get(0);
        String[] key2Split = key2.split("\\.");
        Object value2 = null;
        for (Column c : joinColumnData) {
            if(c.getColumnName().equals(key2Split[1])) {
                value2 = c.getValue();
            }
        }

        return value1 != null && value1.equals(value2);
    }



    /**
     * TODO当前只能做到SELECT后面全部是函数或者全部是字段
     * @return
     */
    private boolean useFunction() {
        // 如果第一个是函数，后面必须都是函数
        if (selectColumns[0].getFunctionName() != null) {
            for (SelectColumn c : selectColumns) {
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
            if (selectColumns.length == 1) {
                return true;
            }
            for (int i = 1; i < selectColumns.length; i++) {
                SelectColumn c = selectColumns[i];
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
            Column[] columnData = row.getColumns();
            boolean matchCondition = conditionTree == null ? true : ConditionComparator.analyzeConditionTree(conditionTree, columnData);
            if (matchCondition && isMatchLimit(currIndex)) {
                Column[] resultColumns = filterColumns(columnData);
                dataList.add(resultColumns);
            }
            if (limit != null && dataList.size() >= limit) {
                return dataList;
            }
            currIndex++;
        }
        return dataList;
    }




    private List<Column[]> getColumnDataListUseIndex(SelectPlan selectPlan, DataChunkStore dataChunkStore) {
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
            if (conditionTree == null) {
                Column[] filterColumns = filterColumns(columnData);
                // 判断是否符合limit、offset
                if (isMatchLimit(currIndex.get())) {
                    dataList.add(filterColumns);
                }
                if (limit != null && dataList.size() >= limit) {
                    break;
                }
                currIndex.addAndGet(1);
            } else {
                boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
                if (compareResult) {
                    Column[] filterColumns = filterColumns(columnData);
                    // 判断是否符合limit、offset
                    if (isMatchLimit(currIndex.get())) {
                        dataList.add(filterColumns);
                    }
                    if (limit != null && dataList.size() >= limit) {
                        break;
                    }
                    currIndex.addAndGet(1);
                }
            }
        }
    }


    /**
     * 是否是符合offset和limit条件的行
     * @param currIndex
     * @return
     */
    private boolean isMatchLimit(int currIndex) {
        if (offset != null && limit != null) {
            int beginIndex = offset;
            int endIndex = offset + limit - 1;
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
                if(conditionTree == null) {
                    // 进入计算函数
                    for (StatFunction statFunction : statFunctions) {
                        statFunction.stat(columnData);
                    }
                    // 按where条件过滤
                } else {
                    boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
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
            Column c = selectColumns[i].getColumn();
            // 日期类型
            if(!selectColumns[i].getFunctionName().equals(FunctionConstant.FUNC_COUNT)
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
                if(conditionTree == null) {
                    Column column = getColumn(columnData, this.groupByColumnName);
                    List<StatFunction> statFunctions = groupByMap.getOrDefault(column, getFunctionList());
                    // 进入计算函数
                    for (StatFunction statFunction : statFunctions) {
                        statFunction.stat(columnData);
                    }
                    groupByMap.put(column, statFunctions);
                } else {
                    boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
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
                Column c = selectColumns[index].getColumn();
                // 日期类型
                if (!selectColumns[index].getFunctionName().equals(FunctionConstant.FUNC_COUNT)
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
        List<StatFunction> statFunctions = new ArrayList<>(selectColumns.length);
        for (SelectColumn selectColumn : selectColumns) {

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
        for (SelectColumn column : selectColumns) {
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
        for (SelectColumn column : selectColumns) {
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

    public ConditionTree getConditionTree() {
        return conditionTree;
    }

    public void setConditionTree(ConditionTree conditionTree) {
        this.conditionTree = conditionTree;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public SelectPlan getSelectPlan() {
        return selectPlan;
    }
    public void setSelectPlan(SelectPlan selectPlan) {
        this.selectPlan = selectPlan;
    }

    public String getGroupByColumnName() {
        return groupByColumnName;
    }

    public void setGroupByColumnName(String groupByColumnName) {
        this.groupByColumnName = groupByColumnName;
    }


    public void setMainTable(TableOperation mainTable) {
        this.mainTable = mainTable;
    }
}
