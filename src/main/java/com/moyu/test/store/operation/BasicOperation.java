package com.moyu.test.store.operation;

import com.moyu.test.command.dml.sql.ConditionTree2;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public abstract class BasicOperation {

    protected ConnectSession session;

    protected String tableName;

    protected Column[] tableColumns;

    protected ConditionTree2 conditionTree;


    public BasicOperation(ConnectSession session, String tableName, Column[] tableColumns, ConditionTree2 conditionTree) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.conditionTree = conditionTree;
    }

    public abstract int insert(RowEntity rowEntity);

    public abstract int update(Column[] updateColumns);

    public abstract int delete();


    public static BasicOperation getEngineOperation(OperateTableInfo tableInfo) {
        if(tableInfo.getEngineType() == null) {
            throw new IllegalArgumentException("引擎类型不能为空");
        }
        BasicOperation basicOperation = null;
        switch (tableInfo.getEngineType()) {
            case CommonConstant.STORE_TYPE_YU:
                basicOperation = new YuEngineOperation(tableInfo);
                break;
            case CommonConstant.STORE_TYPE_YAN:
                basicOperation = new YanEngineOperation(tableInfo);
                break;
            default:
                throw new IllegalArgumentException("不支持数据引擎:" + tableInfo.getEngineType());
        }
        return basicOperation;
    }


}
