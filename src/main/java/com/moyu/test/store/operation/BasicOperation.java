package com.moyu.test.store.operation;

import com.moyu.test.command.dml.sql.ConditionTree;
import com.moyu.test.command.dml.sql.FromTable;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public abstract class BasicOperation {

    protected ConnectSession session;

    protected String tableName;

    protected Column[] tableColumns;

    protected ConditionTree conditionTree;


    public BasicOperation(ConnectSession session, String tableName, Column[] tableColumns, ConditionTree conditionTree) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.conditionTree = conditionTree;
    }

    public abstract int insert(RowEntity rowEntity);

    public abstract int batchFastInsert(List<RowEntity> rowList);

    public abstract int update(Column[] updateColumns);

    public abstract int delete();

    public abstract void createIndex(Integer tableId, String indexName, String columnName, byte indexType);


    public abstract Cursor getQueryCursor(FromTable table) throws IOException;


    public static BasicOperation getEngineOperation(OperateTableInfo tableInfo) {
        if(tableInfo.getEngineType() == null) {
            throw new IllegalArgumentException("引擎类型不能为空");
        }
        BasicOperation basicOperation = null;
        switch (tableInfo.getEngineType()) {
            case CommonConstant.ENGINE_TYPE_YU:
                basicOperation = new YuEngineOperation(tableInfo);
                break;
            case CommonConstant.ENGINE_TYPE_YAN:
                basicOperation = new YanEngineOperation(tableInfo);
                break;
            default:
                throw new IllegalArgumentException("不支持数据引擎:" + tableInfo.getEngineType());
        }
        return basicOperation;
    }


}
