package com.moyu.test.store.operation;

import com.moyu.test.command.dml.expression.Expression;
import com.moyu.test.command.dml.sql.ConditionTree;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class OperateTableInfo {

    private ConnectSession session;

    private String tableName;

    private Column[] tableColumns;

    private ConditionTree conditionTree;

    private Expression condition;

    private List<IndexMetadata> indexList;

    private String engineType = CommonConstant.ENGINE_TYPE_YU;


    public OperateTableInfo(ConnectSession session, String tableName, Column[] tableColumns, Expression condition) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
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

    public ConditionTree getConditionTree() {
        return conditionTree;
    }
    
    public void setIndexList(List<IndexMetadata> indexList) {
        this.indexList = indexList;
    }

    public List<IndexMetadata> getIndexList() {
        return indexList;
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
}
