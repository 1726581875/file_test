package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.session.QueryCacheUtil;
import com.moyu.test.session.Table;
import com.moyu.test.store.operation.BasicOperation;
import com.moyu.test.store.operation.OperateTableInfo;

/**
 * @author xiaomingzhang
 * @date 2023/5/18
 */
public class DeleteCommand extends AbstractCommand {

    private OperateTableInfo tableInfo;

    public DeleteCommand(OperateTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public String execute() {
        BasicOperation engineOperation = BasicOperation.getEngineOperation(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int deleteRowNum = 0;
        synchronized (table) {
            deleteRowNum = engineOperation.delete();
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return "共删除了" + deleteRowNum + "行数据";
    }
}
