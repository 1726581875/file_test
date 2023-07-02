package com.moyu.test.store.operation;

import com.moyu.test.command.dml.sql.ConditionTree2;
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

    private ConditionTree2 conditionTree;

    private List<IndexMetadata> indexList;

    private String engineType = CommonConstant.STORE_TYPE_YU;
    
    

    public OperateTableInfo(ConnectSession session, String tableName, Column[] tableColumns, ConditionTree2 conditionTree) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.conditionTree = conditionTree;
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

    public ConditionTree2 getConditionTree() {
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
}
