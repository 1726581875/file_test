package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.store.common.dto.TableInfo;

/**
 * @author xiaomingzhang
 * @date 2023/5/31
 */
public class CreateIndexCmd extends AbstractCmd {

    private TableInfo tableInfo;

    private String columnName;

    private String indexName;

    private Byte indexType;

    public CreateIndexCmd(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
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
