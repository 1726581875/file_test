package com.moyu.xmz.command.dml.sql;

import com.moyu.xmz.command.dml.InsertCommand;
import com.moyu.xmz.command.dml.expression.ColumnExpression;
import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.expression.SelectColumnExpression;
import com.moyu.xmz.command.dml.expression.SingleComparison;
import com.moyu.xmz.command.dml.function.*;
import com.moyu.xmz.command.dml.plan.SelectIndex;
import com.moyu.xmz.common.config.CommonConfig;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.constant.FunctionConstant;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.common.exception.SqlIllegalException;
import com.moyu.xmz.common.util.PathUtil;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.OperateTableInfo;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.cursor.*;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.type.value.RowValue;

import java.io.IOException;
import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 * 一个查询对象
 */
public class Query {

    private ConnectSession session;

    /**
     * select [selectColumns]
     */
    private SelectColumn[] selectColumns;
    /**
     * from [mainTable]
     */
    private QueryTable mainTable;
    /**
     * where [conditionTree]
     */
    private Expression condition;
    /**
     * 最终选择使用的索引
     */
    private SelectIndex selectIndex;
    /**
     * 可以选择使用的索引列表
     */
    private List<SelectIndex> indexList = new LinkedList<>();
    /**
     * 当前查询最终的结果游标
     */
    private Cursor queryCursor;
    /**
     * GROUP BY [groupFields]
     */
    private List<GroupField> groupFields;
    /**
     *  ORDER BY [orderFields]
     */
    private List<SortField> sortFields;

    /**
     * 带distinct关键字
     * 例如 SELECT DISTINCT name from table
     */
    private boolean isDistinct;

    /**
     * limit [limitValue]
     */
    private Integer limit;
    /**
     * offset [offsetValue]
     */
    private Integer offset = 0;

    /**
     * 收集用到的所有查询游标，以便查询结束后统一关闭
     */
    private List<Cursor> cursorList = new ArrayList<>();


    public Cursor getQueryResultCursor() {
        if (queryCursor == null) {
            Deque<Query> queryStack = new LinkedList<>();
            Query q = this;
            queryStack.push(q);
            while (q.getMainTable() != null && (q = q.getMainTable().getSubQuery()) != null) {
                queryStack.push(q);
            }
            queryCursor = getResultQueryCursor(queryStack);
        }
        return queryCursor;
    }


    private Cursor getResultQueryCursor(Deque<Query> queryStack) {
        Cursor mainCursor = null;

        while (!queryStack.isEmpty()) {
            Query q = queryStack.pop();
            QueryTable fromTable = q.getMainTable();

            if(fromTable == null) {
                mainCursor = getSelectResultNoTable(q);
                return mainCursor;
            }

            if (fromTable.getSubQuery() == null) {
                mainCursor = getMainQueryCursor(q);
            }

            String currTableAlias = q.getMainTable().getAlias();
            if (useFunction(q)) {
                mainCursor = getStatFunctionResult(mainCursor, q);
            } else if (useGroupBy(q)) {
                mainCursor = getGroupByResult(mainCursor, q);
            } else if(isDistinct){
                mainCursor = getDistinctResult(mainCursor, q);
            } else {
                mainCursor = getSimpleQueryResult(mainCursor, q);
            }
            // order by
            if (useOrderBy(q)) {
                mainCursor = getOrderByResult(mainCursor, q);
            }

            Column[] columns = mainCursor.getColumns();
            if (isSubQuery(q)) {
                Column.setColumnTableAlias(columns, currTableAlias);
            }
        }

        return mainCursor;
    }


    /**
     * TODO当前只能做到SELECT后面全部是函数或者全部是字段
     * @return
     */
    private boolean useFunction(Query query) {
        // 如果第一个是统计函数（count、max这类函数），后面必须都是函数
        if (query.getSelectColumns()[0].getFunctionName() != null) {
            for (SelectColumn c : query.getSelectColumns()) {
                if (c.getFunctionName() == null) {
                    throw new SqlIllegalException("sql语法有误");
                }
            }
            return true;
        }
        return false;
    }

    public boolean isUseFunction() {
        // 如果第一个是统计函数（count、max这类函数），后面必须都是函数
        if (this.selectColumns[0].getFunctionName() != null) {
            for (SelectColumn c : this.selectColumns) {
                if (c.getFunctionName() == null) {
                    throw new SqlIllegalException("sql语法有误");
                }
            }
            return true;
        }
        return false;
    }

    private boolean useGroupBy(Query query) {
        if(query.getGroupFields() != null && query.getGroupFields().size() > 0) {
            if (query.getSelectColumns().length == 1) {
                return true;
            }
            for (int i = query.getGroupFields().size(); i < query.getSelectColumns().length; i++) {
                SelectColumn c = query.getSelectColumns()[i];
                if (c.getFunctionName() == null) {
                    throw new SqlIllegalException("sql语法有误");
                }
            }
            return true;
        }
        return false;
    }

    private boolean useOrderBy(Query query) {
        if (query.getSortFields() != null && query.getSortFields().size() > 0) {
            return true;
        }
        return false;
    }


    private Cursor getSimpleQueryResult(Cursor cursor, Query query) {

        // 判断数据是否应该物化(在磁盘生成临时表)
        boolean toDisk = false;
        String diskTemTableName = null;

        List<RowEntity> resultRowList = new ArrayList<>();
        int currIndex = 0;
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            RowEntity rowEntity = null;
            if(isJoinQuery(query)) {
                // 符合这种场景 select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id where b.id = 1 ) t where id = 1;
                // 当前查询为join查询时候，字段的所属的tableAlias(表别名)要保持为连接前原表的别名，以便后面的条件判断和查询字段筛选
                rowEntity = new RowEntity(row.getColumns());
            } else {
                // 符合这种场景select * from (select * from xmz_yan) t where t.id = 1;
                // 非连接条件
                String currTableAlias = query.getMainTable().getAlias();
                rowEntity = new RowEntity(row.getColumns(), currTableAlias);
            }
            rowEntity.setDeleted(row.isDeleted());

            boolean matchCondition = Expression.isMatch(rowEntity, query.getCondition());
            // 只拿符合条件的行
            // 使用order by后，offset limit条件就先不筛选。等后面排完序再筛选行
            if (matchCondition && (useOrderBy(query)  || query.isMatchLimit(query, currIndex))) {
                Column[] resultColumns = query.getResultColumns(rowEntity, query.getSelectColumns());
                //Column[] resultColumns = query.filterColumns(row.getColumns(), query.getSelectColumns());
                resultRowList.add(new RowEntity(resultColumns));
            }
            if ((query.getLimit() != null && resultRowList.size() >= query.getLimit()) && !useOrderBy(query)) {
                break;
            }
            // 数据量大于10000万，对数据进行物化
            if(currIndex >= CommonConfig.MATERIALIZATION_THRESHOLD) {
                toDisk = true;
                // 保存数据到磁盘
                if(resultRowList.size() == CommonConfig.MATERIALIZATION_THRESHOLD) {
                    diskTemTableName = dataToDisk(resultRowList, diskTemTableName);
                    resultRowList.clear();
                }
            }
            currIndex++;
        }

        Cursor resultCursor = null;
        Column[] columns = SelectColumn.getColumnBySelectColumn(query);
        // 判断是否已物化
        if (toDisk) {
            if (resultRowList.size() > 0) {
                diskTemTableName = dataToDisk(resultRowList, diskTemTableName);
                resultRowList.clear();
            }
            try {
                String tmpTablePath = PathUtil.getDataFilePath(session.getDatabaseId(), diskTemTableName);
                resultCursor = new DiskTemTableCursor(tmpTablePath, columns);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            resultCursor = new MemoryTemTableCursor(resultRowList, columns);
        }
        return resultCursor;
    }


    private String dataToDisk(List<RowEntity> resultRowList, String tmpTableName) {
        // 临时表名
        if (tmpTableName == null) {
            tmpTableName =  "tmp_" + session.getSessionId() + "_"
                    + UUID.randomUUID().toString().replace("-", "")
                    + "_" +System.currentTimeMillis();
        }
        // 保存到磁盘
        OperateTableInfo tableInfo = new OperateTableInfo(session, tmpTableName, null, null);
        tableInfo.setEngineType(CommonConstant.ENGINE_TYPE_YU);
        InsertCommand insertCommand = new InsertCommand(tableInfo, null);
        insertCommand.batchFastInsert(resultRowList);
        return tmpTableName;
    }




    private Cursor getStatFunctionResult(Cursor cursor, Query query) {

        List<RowEntity> resultRowList = new ArrayList<>();
        // 1、初始化所有统计函数对象
        List<StatFunction> statFunctions = getFunctionList(query);
        // 2、遍历数据，执行计算函数
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            if (Expression.isMatch(row, query.getCondition())) {
                for (StatFunction statFunction : statFunctions) {
                    statFunction.stat(row.getColumns());
                }
            }
        }
        // 处理执行结果
        Column[] resultColumns = new Column[statFunctions.size()];
        for (int i = 0; i < statFunctions.size(); i++) {
            StatFunction statFunction = statFunctions.get(i);
            resultColumns[i] = functionResultToColumn(statFunction, i, query);
        }
        resultRowList.add(new RowEntity(resultColumns));

        Column[] columns = SelectColumn.getColumnBySelectColumn(query);
        Cursor resultCursor = new MemoryTemTableCursor(resultRowList,columns);
        return resultCursor;
    }

    private Cursor getSelectResultNoTable(Query query) {
        SelectColumn[] selectColumns = query.getSelectColumns();
        Column[] resultColumns = new Column[selectColumns.length];
        for (int i = 0; i < selectColumns.length; i++){
            SelectColumnExpression columnExpression = selectColumns[i].getColumnExpression();
            resultColumns[i] = (Column)columnExpression.getValue(null);
        }
        List<RowEntity> resultRowList = new ArrayList<>();
        resultRowList.add(new RowEntity(resultColumns));
        // 字段元数据信息
        Column[] metaColumns = resultRowList.get(0).getColumns();
        Cursor resultCursor = new MemoryTemTableCursor(resultRowList, metaColumns);
        return resultCursor;
    }



    private Cursor getGroupByResult(Cursor cursor, Query query) {

        List<RowEntity> resultRowList = new ArrayList<>();
        List<GroupField> groupFields = query.getGroupFields();
        Map<GroupMapKey, List<StatFunction>> groupByMap = new HashMap<>();
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            if (Expression.isMatch(row, query.getCondition())) {
                List<Column> groupByColumns = new LinkedList<>();
                for (GroupField groupField : groupFields) {
                    Column groupByColumn = groupField.getColumn().copy();
                    Column column = getColumn(row.getColumns(), groupByColumn.getColumnName());
                    groupByColumn.setValue(column.getValue());
                    groupByColumns.add(groupByColumn);
                }
                GroupMapKey groupMapKey = new GroupMapKey(groupByColumns);

                List<StatFunction> statFunctions = groupByMap.getOrDefault(groupMapKey, getFunctionList(query));
                // 进入计算函数
                for (StatFunction statFunction : statFunctions) {
                    statFunction.stat(row.getColumns());
                }
                groupByMap.put(groupMapKey, statFunctions);
            }
        }

        // 汇总执行结果
        int rowIndex = 0;
        for (Map.Entry<GroupMapKey, List<StatFunction>> entry : groupByMap.entrySet()) {
            List<Column> groupByColumns = entry.getKey().getColumnList();
            List<StatFunction> statFunctions = entry.getValue();
            Column[] resultColumns = new Column[groupByColumns.size() + statFunctions.size()];
            // group by字段
            for (int i = 0; i < groupByColumns.size(); i++) {
                resultColumns[i] = groupByColumns.get(i);
            }
            // 其余字段为统计函数
            for (int i = 0; i < statFunctions.size(); i++) {
                int index = groupByColumns.size() + i;
                StatFunction statFunction = statFunctions.get(i);
                resultColumns[index] = functionResultToColumn(statFunction, index, query);
            }
            // 如果有offset、limit限制，判断是否符合条件
            if(query.isMatchLimit(query, rowIndex)) {
                resultRowList.add(new RowEntity(resultColumns));
                rowIndex++;
            } else {
                break;
            }

        }
        Column[] columns = SelectColumn.getColumnBySelectColumn(query);
        Cursor resultCursor = new MemoryTemTableCursor(resultRowList,columns);
        return resultCursor;
    }

    private Cursor getOrderByResult(Cursor cursor, Query query) {
        List<RowEntity> resultRowList = new LinkedList<>();

        List<SortField> sortFields = query.getSortFields();
        int[] columnIndexMap = new int[sortFields.size()];
        Column[] tableColumns = cursor.getColumns();
        // 确认排序字段字段下标，方便后面取值比较
        for (int i = 0; i < sortFields.size(); i++) {
            for (int j = 0; j < tableColumns.length; j++) {
                if(tableColumns[j].metaEquals(sortFields.get(i).getColumn())) {
                    columnIndexMap[i] = j;
                }
            }
        }
        // 遍历所有行
        boolean useDiskSort = false;
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            if (Expression.isMatch(row, query.getCondition())) {
                if(!useDiskSort) {
                    resultRowList.add(row);
                } else {
                    // TODO 磁盘排序逻辑

                }
                // 如果行数超过内存排序的最大值，启用磁盘排序
/*                if(resultRowList.size() > CommonConfig.MAX_MEMORY_SORT_ROW_SIZE) {
                    useDiskSort = true;
                    resultRowList.clear();
                }*/

            }
        }

        Cursor resultCursor = null;
        if (useDiskSort) {
            // TODO 待完善
        } else {
            // 内存排序
            memorySort(resultRowList, columnIndexMap);

            Column[] columns = SelectColumn.getColumnBySelectColumn(query);
            resultCursor = new MemoryTemTableCursor(resultRowList, columns);
            // 如果有limit语句，需要进行筛选
            if (query.getLimit() != null) {
                List<RowEntity> rowList = new ArrayList<>(query.getLimit());
                RowEntity r = null;
                int i = 0;
                while ((r = resultCursor.next()) != null) {
                    if (isMatchLimit(query, i)) {
                        rowList.add(r);
                    }
                    if (query.getLimit() != null && rowList.size() >= query.getLimit()) {
                        break;
                    }
                    i++;
                }
                resultCursor = new MemoryTemTableCursor(rowList, columns);
            }
        }

        return resultCursor;
    }


    private void memorySort(List<RowEntity> resultRowList, int[] columnIndexMap){
        //  内存排序
        Collections.sort(resultRowList, (r1, r2) -> {
            for (int i = 0; i < columnIndexMap.length; i++) {
                SortField sortField = sortFields.get(i);
                Column c1 = r1.getColumns(columnIndexMap[i]);
                Column c2 = r2.getColumns(columnIndexMap[i]);
                if(!c1.getValue().equals(c2.getValue())) {
                    int r = ((Comparable) c1.getValue()).compareTo(c2.getValue());
                    if(SortField.RULE_ASC.equals(sortField.getType())) {
                        return r;
                    } else {
                        return -r;
                    }
                }
            }
            return 0;
        });
    }





    private Cursor getDistinctResult(Cursor cursor, Query query) {

        // 去重
        Set<RowEntity> distinctSet = new HashSet<>();
        RowEntity row = null;
        while ((row = cursor.next()) != null) {
            if (Expression.isMatch(row, query.getCondition())) {
                //RowEntity rowEntity = filterColumns(row, query.getSelectColumns());
                Column[] resultColumns = getResultColumns(row, query.getSelectColumns());
                RowEntity rowEntity = new RowEntity(resultColumns);
                if(!distinctSet.contains(rowEntity)) {
                    distinctSet.add(rowEntity);
                }
            }
        }

        // 最终结果
        int currIndex = 0;
        List<RowEntity> resultRowList = new ArrayList<>();
        for (RowEntity rowEntity : distinctSet) {
            if(query.isMatchLimit(query, currIndex)) {
                resultRowList.add(rowEntity);
                currIndex++;
            }
            if (query.getLimit() != null && resultRowList.size() >= query.getLimit()) {
                break;
            }
        }
        Column[] columns = SelectColumn.getColumnBySelectColumn(query);
        Cursor resultCursor = new MemoryTemTableCursor(resultRowList,columns);
        return resultCursor;
    }

    private List<StatFunction> getFunctionList(Query query) {
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
                    String[] split = columnName.split("\\s+");
                    if(split.length > 1) {
                        statFunctions.add(new CountFunction(split[1], true));
                    } else {
                        statFunctions.add(new CountFunction(split[0]));
                    }
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
                case FunctionConstant.FUNC_AVG:
                    statFunctions.add(new AvgFunction(columnName));
                    break;
                default:
                    throw new SqlIllegalException("sql语法错误，不支持该函数：" + functionName);
            }
        }
        return statFunctions;
    }


    private Column functionResultToColumn(StatFunction statFunction, int columnIndex, Query query) {

        Column resultColumn = null;
        SelectColumn selectColumn = query.getSelectColumns()[columnIndex];
        Column c = selectColumn.getColumn();
        String columnName  = selectColumn.getAlias() != null ? selectColumn.getAlias() : selectColumn.getSelectColumnName();

        Class<? extends StatFunction> fClass = statFunction.getClass();
        if (CountFunction.class.equals(fClass)
                || SumFunction.class.equals(fClass)
                || MinFunction.class.equals(fClass)
                || MaxFunction.class.equals(fClass)) {
            Long statResult = statFunction.getValue();

            // 日期类型
            if (!FunctionConstant.FUNC_COUNT.equals(selectColumn.getFunctionName())
                    && c != null && c.getColumnType() == ColumnTypeConstant.TIMESTAMP) {
                resultColumn = new Column(columnName, ColumnTypeConstant.TIMESTAMP, columnIndex, 8);
                resultColumn.setValue(statResult == null ? null : new Date(statResult));
            } else {
                // 数字类型
                resultColumn = new Column(columnName, ColumnTypeConstant.INT_8, columnIndex, 8);
                resultColumn.setValue(statResult);
            }
        } else if (AvgFunction.class.equals(fClass)) {
            AvgFunction avgFunction = (AvgFunction) statFunction;
            Double avgValue = avgFunction.getAvgValue();
            // TODO 应当为Double类型
            resultColumn = new Column(columnName, ColumnTypeConstant.INT_8, columnIndex, 8);
            resultColumn.setValue(avgValue);
        } else {
            throw new SqlIllegalException("sql语法错误，不支持该函数" + statFunction);
        }




        return resultColumn;
    }

    private Column getColumn(Column[] columns, String columnName) {
        for (Column c : columns) {
            if (c.getColumnName().equals(columnName)) {
                return c;
            }
        }
        return null;
    }

    /**
     * 是否是符合query.getOffset()和query.getLimit()条件的行
     * @param currIndex
     * @return
     */
    public boolean isMatchLimit(Query query, int currIndex) {
        if (query.getOffset() != null && query.getLimit() != null) {
            int beginIndex = query.getOffset();
            int endIndex = query.getOffset() + query.getLimit() - 1;
            if (currIndex < beginIndex || currIndex > endIndex) {
                return false;
            }
        }
        return true;
    }


    public Column[] getResultColumns(RowEntity rowEntity, SelectColumn[] selectColumns) {
        Column[] resultColumns = new Column[selectColumns.length];
        for (int i = 0; i < selectColumns.length; i++) {
            SelectColumn selectColumn = selectColumns[i];
            SelectColumnExpression columnExpression = selectColumn.getColumnExpression();
            Column columnValue = (Column) columnExpression.getValue(rowEntity);
            resultColumns[i] = columnValue;
        }
        return resultColumns;
    }



    private boolean isJoinQuery(Query q){
        return q.getMainTable().getJoinTables() != null && q.getMainTable().getJoinTables().size() > 0;
    }

    private boolean isSubQuery(Query q) {
        return q.getMainTable().getSubQuery() != null;
    }


    private Cursor getMainQueryCursor(Query query) {
        Cursor mainCursor = null;
        try {
            // 主表
            QueryTable mTable = query.getMainTable();
            mainCursor = getQueryCursor(mTable);
            List<QueryTable> joinTables = mTable.getJoinTables();
            // 如果存在连接表则进行join操作
            if (joinTables != null && joinTables.size() > 0) {
                for (QueryTable joinTable : joinTables) {
                    Cursor joinCursor = null;
                    try {
                        // join table
                        joinCursor = getQueryCursor(joinTable);
                        Expression joinCondition = joinTable.getJoinCondition();
                        if((joinCursor instanceof BtreeCursor)
                                && isPrimaryKey(joinCondition, joinTable.getTableColumns())) {
                            mainCursor = indexJoinTable(mainCursor, (BtreeCursor) joinCursor, joinCondition , joinTable.getJoinInType());
                        } else {
                            mainCursor = doJoinTable(mainCursor, joinCursor, joinTable.getJoinCondition(), joinTable.getJoinInType());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new SqlExecutionException("连接查询异常");
                    } finally {
                        if(joinCursor != null) {
                            joinCursor.close();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new SqlExecutionException("查询异常");
        }
        return mainCursor;
    }


    private boolean isPrimaryKey(Expression joinCondition, Column[] columns){
/*        if(joinCondition instanceof ConditionLeftRight) {
            Column right = ((ConditionLeftRight) joinCondition).getRight();
            for (Column c : columns) {
                if(c.getColumnName().equals(right.getColumnName())
                        && c.getIsPrimaryKey() == CommonConstant.PRIMARY_KEY) {
                    return true;
                }
            }
        }*/
        return false;
    }


    public Cursor getQueryCursor(QueryTable table) throws IOException {
        OperateTableInfo tableInfo = new OperateTableInfo(session, table.getTableName(), table.getTableColumns(), null);
        tableInfo.setEngineType(table.getEngineType());
        StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
        Cursor queryCursor = engineOperation.getQueryCursor(table);
        cursorList.add(queryCursor);
        return queryCursor;
    }


    private Cursor doJoinTable(Cursor leftCursor, Cursor rightCursor, Expression joinCondition, String joinType) {

        // 字段元数据
        Column[] columns = Column.mergeColumns(leftCursor.getColumns(), rightCursor.getColumns());

        List<RowEntity> resultList = new LinkedList<>();
        // 内连接、左连接
        if (CommonConstant.JOIN_TYPE_INNER.equals(joinType)
                || CommonConstant.JOIN_TYPE_LEFT.equals(joinType)) {
            RowEntity leftRow = null;
            while ((leftRow = leftCursor.next()) != null) {
                List<RowEntity> rows = new LinkedList<>();
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


    private Cursor indexJoinTable(Cursor leftCursor, BtreeCursor rightCursor, Expression joinCondition, String joinType) {
        // 字段元数据
        Column[] columns = Column.mergeColumns(leftCursor.getColumns(), rightCursor.getColumns());

        BTreeMap bTreeMap = rightCursor.getBTreeMap();
        List<RowEntity> resultList = new LinkedList<>();
        // 内连接、左连接
        if (CommonConstant.JOIN_TYPE_INNER.equals(joinType)
                || CommonConstant.JOIN_TYPE_LEFT.equals(joinType)) {
            RowEntity leftRow = null;
            while ((leftRow = leftCursor.next()) != null) {

                Object leftKeyValue = getLeftKeyValue(leftRow, joinCondition);
                RowValue rowValue = (RowValue) bTreeMap.get(leftKeyValue);
                List<RowEntity> rows = new LinkedList<>();
                if(rowValue != null) {
                    RowEntity rightRow = rowValue.getRowEntity(rightCursor.getColumns());
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


    private Object getLeftKeyValue(RowEntity leftRow, Expression joinCondition) {

        if(joinCondition instanceof SingleComparison) {
            SingleComparison leftRight = (SingleComparison) condition;
            // 左表字段
            Expression left = (ColumnExpression)leftRight.getLeft();
            return left.getValue(leftRow);
        } else {
            throw new DbException("不支持连接条件");
        }
    }


    private boolean isMatchJoinCondition(RowEntity leftRow, RowEntity rightRow, Expression joinCondition) {
        RowEntity rowEntity = RowEntity.mergeRow(leftRow, rightRow);
        return Expression.isMatch(rowEntity, joinCondition);
    }


    public void resetQueryCursor() {
        cursorList = new ArrayList<>();
        queryCursor = null;
    }


    public void closeQuery() {

        if (cursorList != null && cursorList.size() > 0) {
            for (Cursor cursor : cursorList) {
                cursor.close();
            }
        }
        if(queryCursor != null) {
            queryCursor.close();
        }
    }


    public SelectColumn[] getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(SelectColumn[] selectColumns) {
        this.selectColumns = selectColumns;
    }

    public QueryTable getMainTable() {
        return mainTable;
    }

    public void setMainTable(QueryTable mainTable) {
        this.mainTable = mainTable;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    public ConnectSession getSession() {
        return session;
    }

    public void setSession(ConnectSession session) {
        this.session = session;
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

    public SelectIndex getSelectIndex() {
        return selectIndex;
    }

    public void setSelectIndex(SelectIndex selectIndex) {
        this.selectIndex = selectIndex;
    }


    public void setDistinct(boolean distinct) {
        isDistinct = distinct;
    }

    public List<SelectIndex> getIndexList() {
        return indexList;
    }

    public List<SortField> getSortFields() {
        return sortFields;
    }

    public void setSortFields(List<SortField> sortFields) {
        this.sortFields = sortFields;
    }

    public void setGroupFields(List<GroupField> groupFields) {
        this.groupFields = groupFields;
    }

    public List<GroupField> getGroupFields() {
        return groupFields;
    }
}
