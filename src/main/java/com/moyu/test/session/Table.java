package com.moyu.test.session;

import com.moyu.test.exception.DbException;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.store.metadata.obj.TableMetadata;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/15
 */
public class Table {

    private Integer databaseId;

    private Integer tableId;

    private String tableName;

    private Column[] columns;

    private String engineType;


    public Table(TableMetadata tableMetadata) {
        this.tableId = tableMetadata.getTableId();
        this.tableName = tableMetadata.getTableName();
        this.databaseId = tableMetadata.getDatabaseId();
        this.engineType = tableMetadata.getEngineType();
    }

    public String getTableName() {
        return tableName;
    }


    public Column[] getColumns() {
        if(columns == null || columns.length == 0) {
            this.columns = getColumns(tableId, tableName);
        }
        return columns;
    }

    private Column[] getColumns(Integer databaseId, String tableName) {
        List<ColumnMetadata> columnMetadataList = null;
        TableMetadataStore tableMetadata = null;
        ColumnMetadataStore columnStore = null;
        try {
            tableMetadata = new TableMetadataStore(databaseId);
            columnStore = new ColumnMetadataStore();
            TableMetadata table = tableMetadata.getTable(tableName);
            if(table == null) {
                throw new SqlExecutionException("表" + tableName + "不存在");
            }
            TableColumnBlock columnBlock = columnStore.getColumnBlock(table.getTableId());
            columnMetadataList = columnBlock.getColumnMetadataList();
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DbException("获取字段信息发生异常");
        } finally {
            columnStore.close();
            tableMetadata.close();
        }

        if(columnMetadataList == null || columnMetadataList.size() == 0) {
            throw new SqlIllegalException("表缺少字段");
        }

        Column[] columns = new Column[columnMetadataList.size()];

        for (int i = 0; i < columnMetadataList.size(); i++) {
            ColumnMetadata metadata = columnMetadataList.get(i);
            Column column = new Column(metadata.getColumnName(),
                    metadata.getColumnType(),
                    metadata.getColumnIndex(),
                    metadata.getColumnLength());
            column.setIsPrimaryKey(metadata.getIsPrimaryKey());
            columns[i] = column;
        }

        return columns;
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public String getEngineType() {
        return engineType;
    }
}
