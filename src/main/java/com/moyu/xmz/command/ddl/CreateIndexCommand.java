package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.OperateTableInfo;

/**
 * @author xiaomingzhang
 * @date 2023/5/31
 */
public class CreateIndexCommand extends AbstractCommand {

    private OperateTableInfo tableInfo;

    private String columnName;

    private String indexName;

    private Byte indexType;

    public CreateIndexCommand(OperateTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngineOperation(tableInfo);
        engineOperation.createIndex(tableInfo.getTable().getTableId(), indexName, columnName, indexType);
        return QueryResult.simpleResult(RESULT_OK);
    }


    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setIndexType(Byte indexType) {
        this.indexType = indexType;
    }
}
