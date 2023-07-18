package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.session.QueryCacheUtil;
import com.moyu.test.session.Table;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.operation.BasicOperation;
import com.moyu.test.store.operation.OperateTableInfo;

/**
 * @author xiaomingzhang
 * @date 2023/5/20
 */
public class UpdateCommand extends AbstractCommand {

    private Column[] updateColumns;

    private OperateTableInfo tableInfo;


    public UpdateCommand(OperateTableInfo tableInfo, Column[] updateColumns) {
        this.tableInfo = tableInfo;
        this.updateColumns = updateColumns;
    }

    @Override
    public String execute() {
        BasicOperation engineOperation = BasicOperation.getEngineOperation(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int updateRowNum = 0;
        synchronized (table) {
            updateRowNum = engineOperation.update(updateColumns);
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return "共更新了" + updateRowNum + "行数据";
    }

}
