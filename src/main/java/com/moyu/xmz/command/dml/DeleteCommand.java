package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.session.QueryCacheUtil;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.common.dto.OperateTableInfo;

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
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngineOperation(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int deleteRowNum = 0;
        synchronized (table) {
            deleteRowNum = engineOperation.delete();
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return QueryResult.simpleResult("共删除了" + deleteRowNum + "行数据");
    }
}
