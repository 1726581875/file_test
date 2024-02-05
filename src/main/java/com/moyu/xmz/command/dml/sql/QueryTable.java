package com.moyu.xmz.command.dml.sql;

import com.moyu.xmz.command.dml.plan.SelectIndex;
import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.IndexMeta;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/6/7
 */
public class QueryTable {
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
     * 连接类型
     * LEFT 左连接
     * INNER 内连接
     * RIGHT 右连接
     */
    private String joinInType;
    /**
     * 连接条件
     */
    private Expression joinCondition;
    /**
     * 连接的表
     */
    private List<QueryTable> joinTables;
    /**
     * 子查询
     */
    private Query subQuery;
    /**
     * 最终选择使用的索引
     */
    private SelectIndex selectIndex;
    /**
     * 可以选择使用的索引列表
     */
    private List<SelectIndex> indexList = new LinkedList<>();
    /**
     * 当前表所有索引map
     */
    private Map<String, IndexMeta> indexMap;
    /**
     * 存储引擎类型
     */
    private String engineType;


    public QueryTable(String tableName, Column[] tableColumns) {
        this.tableName = tableName;
        this.tableColumns = tableColumns;
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

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public String getEngineType() {
        return engineType;
    }

    public String getJoinInType() {
        return joinInType;
    }

    public void setJoinInType(String joinInType) {
        this.joinInType = joinInType;
    }

    public List<QueryTable> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<QueryTable> joinTables) {
        this.joinTables = joinTables;
    }

    public Expression getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(Expression joinCondition) {
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

    public void setSelectIndex(SelectIndex selectIndex) {
        this.selectIndex = selectIndex;
    }

    public SelectIndex getSelectIndex() {
        if (selectIndex == null && indexList != null && indexList.size() > 0) {
            return indexList.get(0);
        }
        return selectIndex;
    }

    public List<SelectIndex> getIndexList() {
        return indexList;
    }

    public void setIndexMap(Map<String, IndexMeta> indexMap) {
        this.indexMap = indexMap;
    }

    public Map<String, IndexMeta> getIndexMap() {
        return indexMap;
    }
}
