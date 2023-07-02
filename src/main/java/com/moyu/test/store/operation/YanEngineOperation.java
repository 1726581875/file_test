package com.moyu.test.store.operation;

import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class YanEngineOperation extends BasicOperation {


    public YanEngineOperation(OperateTableInfo tableInfo) {
        super(tableInfo.getSession(), tableInfo.getTableName(), tableInfo.getTableColumns(), tableInfo.getConditionTree());
    }


    @Override
    public int insert(RowEntity rowEntity) {
        return 0;
    }

    @Override
    public int update(Column[] updateColumns) {
        return 0;
    }

    @Override
    public int delete() {
        return 0;
    }
}
