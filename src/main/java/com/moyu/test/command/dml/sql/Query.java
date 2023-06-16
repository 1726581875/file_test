package com.moyu.test.command.dml.sql;

import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.plan.SelectIndex;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.DefaultCursor;
import com.moyu.test.store.data.cursor.MemoryTemTableCursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.SelectColumn;
import com.moyu.test.util.PathUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

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
    private FromTable mainTable;
    /**
     * where [conditionTree]
     */
    private ConditionTree2 conditionTree;

    private SelectIndex selectIndex;

    private Cursor queryCursor;

    private String groupByColumnName;

    private Integer limit;

    private Integer offset = 0;


    public SelectColumn[] getSelectColumns() {
        return selectColumns;
    }

    public void setSelectColumns(SelectColumn[] selectColumns) {
        this.selectColumns = selectColumns;
    }

    public FromTable getMainTable() {
        return mainTable;
    }

    public void setMainTable(FromTable mainTable) {
        this.mainTable = mainTable;
    }

    public ConditionTree2 getConditionTree() {
        return conditionTree;
    }

    public void setConditionTree(ConditionTree2 conditionTree) {
        this.conditionTree = conditionTree;
    }

    public Cursor getQueryCursor() {
        if (queryCursor == null) {
            Stack<Query> queryStack = new Stack<>();
            Query q = this;
            queryStack.add(q);
            while ((q = q.getMainTable().getSubQuery()) != null) {
                queryStack.add(q);
            }
            queryCursor = execQuery(queryStack);
        }
        return queryCursor;
    }

    private Cursor execQuery(Stack<Query> queryStack) {
        Cursor mainCursor = null;

        while (!queryStack.isEmpty()) {
            Query q = queryStack.pop();
            FromTable fromTable = q.getMainTable();
            if(fromTable.getSubQuery() == null) {
                mainCursor = getQueryCursor(q);
            }

            int currIndex = 0;
            RowEntity mainRow = null;
            String currTableAlias = q.getMainTable().getAlias();
            List<RowEntity> rowEntityList = new ArrayList<>();
            while ((mainRow = mainCursor.next()) != null) {

                RowEntity rowEntity = null;
                if(isJoinQuery(q)) {
                    // 符合这种场景 select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id where b.id = 1 ) t where id = 1;
                    // 当前查询为join查询时候，字段的所属的tableAlias(表别名)要保持为连接前原表的别名，以便后面的条件判断和查询字段筛选
                    rowEntity = new RowEntity(mainRow.getColumns());
                } else {
                    // 符合这种场景select * from (select * from xmz_yan) t where t.id = 1;
                    // 非连接条件
                    rowEntity = new RowEntity(mainRow.getColumns(), currTableAlias);
                }

                if (ConditionComparator.isMatch(rowEntity, q.getConditionTree()) && isMatchLimit(q, currIndex)) {
                    RowEntity row = filterColumns(rowEntity, q.getSelectColumns());
                    if(isSubQuery(q)) {
                        row = row.setTableAlias(currTableAlias);
                    }
                    rowEntityList.add(row);
                }
                if (q.getLimit() != null && rowEntityList.size() >= q.getLimit()) {
                    break;
                }
                currIndex++;
            }

            Column[] columns = mainCursor.getColumns();
            if(isSubQuery(q)) {
                Column.setColumnAlias(columns, currTableAlias);
            }
            mainCursor = new MemoryTemTableCursor(rowEntityList, columns);
        }

        return mainCursor;
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


    private RowEntity filterColumns(RowEntity row, SelectColumn[] selectColumns) {
        Column[] columns = filterColumns(row.getColumns(), selectColumns);
        return new RowEntity(columns);
    }


    public Column[] filterColumns(Column[] columnData, SelectColumn[] selectColumns) {
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

    private boolean isJoinQuery(Query q){
        return q.getMainTable().getJoinTables() != null && q.getMainTable().getJoinTables().size() > 0;
    }

    private boolean isSubQuery(Query q) {
        return q.getMainTable().getSubQuery() != null;
    }



    private Cursor getQueryCursor(Query query) {

        DataChunkStore mainTableStore = null;
        Cursor mainCursor = null;
        try {
            FromTable mTable = query.getMainTable();
            mainTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.session.getDatabaseId(), mTable.getTableName()));
            mainCursor = new DefaultCursor(mainTableStore, mTable.getTableColumns());

            List<FromTable> joinTables = mTable.getJoinTables();
            // join table
            if(joinTables != null && joinTables.size() > 0) {
                for (FromTable joinTable : joinTables) {
                    DataChunkStore joinTableStore = null;
                    try {
                        joinTableStore = new DataChunkStore(PathUtil.getDataFilePath(this.session.getDatabaseId(), joinTable.getTableName()));
                        DefaultCursor joinCursor = new DefaultCursor(joinTableStore, joinTable.getTableColumns());
                        // 进行连表操作
                        mainCursor = doJoinTable(mainCursor, joinCursor, joinTable.getJoinCondition(), joinTable.getJoinInType());
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


    private Cursor doJoinTable(Cursor leftCursor, Cursor rightCursor, ConditionTree2 joinCondition, String joinType) {

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


    public ConnectSession getSession() {
        return session;
    }

    public void setSession(ConnectSession session) {
        this.session = session;
    }

    public String getGroupByColumnName() {
        return groupByColumnName;
    }

    public void setGroupByColumnName(String groupByColumnName) {
        this.groupByColumnName = groupByColumnName;
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
}
