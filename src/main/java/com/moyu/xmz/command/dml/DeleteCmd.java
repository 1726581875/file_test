package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.session.QueryCacheUtil;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.common.dto.TableInfo;

/**
 * @author xiaomingzhang
 * @date 2023/5/18
 */
public class DeleteCmd extends AbstractCmd {

    private TableInfo tableInfo;

    public DeleteCmd(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int deleteRowNum = 0;
        synchronized (table) {
            deleteRowNum = engineOperation.delete();
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return QueryResult.simpleResult("共删除了" + deleteRowNum + "行数据");
    }
}
