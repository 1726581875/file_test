package com.moyu.test.command.dml.sql;

import com.moyu.test.command.dml.plan.SelectIndex;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.metadata.obj.SelectColumn;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 * 一个查询对象
 */
public class Query {

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
        return queryCursor;
    }

    public void setQueryCursor(Cursor queryCursor) {
        this.queryCursor = queryCursor;
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
