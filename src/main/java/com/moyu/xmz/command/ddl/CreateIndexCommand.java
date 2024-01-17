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

    private Integer databaseId;

    private Integer tableId;

    private String tableName;

    private String columnName;

    private String indexName;

    private Byte indexType;

    private Column[] columns;

    private Column indexColumn;

    public CreateIndexCommand(OperateTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public QueryResult execCommand() {
        StoreEngine engineOperation = StoreEngine.getEngineOperation(tableInfo);
        engineOperation.createIndex(tableId, indexName, columnName, indexType);
        return QueryResult.simpleResult(RESULT_OK);
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public Column getIndexColumn() {
        return indexColumn;
    }

    public void setIndexColumn(Column indexColumn) {
        this.indexColumn = indexColumn;
    }

    public Byte getIndexType() {
        return indexType;
    }

    public void setIndexType(Byte indexType) {
        this.indexType = indexType;
    }
}
