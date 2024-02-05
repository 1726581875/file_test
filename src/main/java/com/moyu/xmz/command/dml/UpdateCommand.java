package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.session.QueryCacheUtil;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.OperateTableInfo;

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
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int updateRowNum = 0;
        synchronized (table) {
            updateRowNum = engineOperation.update(updateColumns);
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return QueryResult.simpleResult("共更新了" + updateRowNum + "行数据");
    }

}
