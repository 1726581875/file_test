package com.moyu.xmz.store.common.dto;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.common.meta.IndexMetadata;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class OperateTableInfo {

    private ConnectSession session;

    private Table table;

    private String tableName;

    private Column[] tableColumns;

    private Expression condition;

    private List<IndexMetadata> allIndexList;

    private String engineType = CommonConstant.ENGINE_TYPE_YU;


    public OperateTableInfo(ConnectSession session, String tableName, Column[] tableColumns, Expression condition) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.condition = condition;
    }

    public OperateTableInfo(ConnectSession session, Table table, Expression condition) {
        this.session = session;
        this.table = table;
        this.engineType = table.getEngineType();
        this.tableName = table.getTableName();
        this.tableColumns = table.getColumns();
        this.condition = condition;
    }


    public ConnectSession getSession() {
        return session;
    }

    public String getTableName() {
        return tableName;
    }

    public Column[] getTableColumns() {
        return tableColumns;
    }

    
    public void setAllIndexList(List<IndexMetadata> allIndexList) {
        this.allIndexList = allIndexList;
    }

    public List<IndexMetadata> getAllIndexList() {
        return allIndexList;
    }

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public Expression getCondition() {
        return condition;
    }

    public Table getTable() {
        return table;
    }
}
