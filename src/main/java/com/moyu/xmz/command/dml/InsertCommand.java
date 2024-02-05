package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.session.QueryCacheUtil;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.OperateTableInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class InsertCommand extends AbstractCommand {

    private OperateTableInfo tableInfo;

    private Column[] dataColumns;


    public InsertCommand(OperateTableInfo tableInfo, Column[] dataColumns) {
        this.tableInfo = tableInfo;
        this.dataColumns = dataColumns;
    }

    @Override
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int num = 0;
        synchronized (table) {
            num = engineOperation.insert(new RowEntity(dataColumns));
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return num == 1 ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }


    public String batchFastInsert(List<RowEntity> rowEntityList) {
        StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
        int num = engineOperation.batchFastInsert(rowEntityList);
        return num == 1 ? "ok" : "error";
    }

    public String batchWriteList(List<Column[]> columnsList) {
        List<RowEntity> rowEntityList = columnsList.stream().map(RowEntity::new).collect(Collectors.toList());
        return batchFastInsert(rowEntityList);
    }
}
