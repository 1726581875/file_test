package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.session.QueryCacheUtil;
import com.moyu.test.session.Table;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.operation.BasicOperation;
import com.moyu.test.store.operation.OperateTableInfo;

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
        BasicOperation engineOperation = BasicOperation.getEngineOperation(tableInfo);
        Table table = tableInfo.getSession().getDatabase().getTable(tableInfo.getTableName());
        int num = 0;
        synchronized (table) {
            num = engineOperation.insert(new RowEntity(dataColumns));
        }
        QueryCacheUtil.clearQueryCache(tableInfo.getSession().getDatabaseId());
        return num == 1 ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }


    public String batchFastInsert(List<RowEntity> rowEntityList) {
        BasicOperation engineOperation = BasicOperation.getEngineOperation(tableInfo);
        int num = engineOperation.batchFastInsert(rowEntityList);
        return num == 1 ? "ok" : "error";
    }

    public String batchWriteList(List<Column[]> columnsList) {
        List<RowEntity> rowEntityList = columnsList.stream().map(RowEntity::new).collect(Collectors.toList());
        return batchFastInsert(rowEntityList);
    }
}
