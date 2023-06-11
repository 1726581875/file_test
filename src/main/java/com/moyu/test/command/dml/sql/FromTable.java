package com.moyu.test.command.dml.sql;

import com.moyu.test.store.metadata.obj.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/7
 */
public class FromTable {
    /**
     * 表名
     */
    private String tableName;
    /**
     * 表别名
     */
    private String alias;
    /**
     * 所有字段信息,包含连接表字段
     */
    private Column[] allColumns;

    /**
     * 当前表所有字段信息
     */
    private Column[] tableColumns;

    /**
     * 查询条件
     */
    private ConditionTree2 tableCondition;
    /**
     * 连接类型
     * LEFT 左连接
     * INNER 内连接
     */
    private String joinInType;
    /**
     * 连接条件
     */
    private ConditionTree2 joinCondition;
    /**
     * 连接的表
     */
    private List<FromTable> joinTables;
    /**
     * 子查询
     */
    private Query subQuery;


    public FromTable(String tableName, Column[] tableColumns, ConditionTree2 tableCondition) {
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.tableCondition = tableCondition;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public ConditionTree2 getTableCondition() {
        return tableCondition;
    }

    public void setTableCondition(ConditionTree2 tableCondition) {
        this.tableCondition = tableCondition;
    }

    public String getJoinInType() {
        return joinInType;
    }

    public void setJoinInType(String joinInType) {
        this.joinInType = joinInType;
    }

    public List<FromTable> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<FromTable> joinTables) {
        this.joinTables = joinTables;
    }

    public ConditionTree2 getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(ConditionTree2 joinCondition) {
        this.joinCondition = joinCondition;
    }

    public Column[] getAllColumns() {
        return allColumns;
    }

    public void setAllColumns(Column[] allColumns) {
        this.allColumns = allColumns;
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
    }

    public Column[] getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(Column[] tableColumns) {
        this.tableColumns = tableColumns;
    }
}
