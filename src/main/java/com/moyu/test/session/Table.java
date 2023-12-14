package com.moyu.test.session;

import com.moyu.test.exception.DbException;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.store.metadata.obj.TableMetadata;
import com.moyu.test.util.CollectionUtils;

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

    public Table(Integer databaseId, String tableName) {
        init(databaseId, tableName);
    }

    public String getTableName() {
        return tableName;
    }


    public Column[] getColumns() {
        if(columns == null || columns.length == 0) {
            init(databaseId, tableName);
        }
        return columns;
    }

    private void init(Integer databaseId, String tableName) {

        TableMetadata tableMetadata = null;
        List<ColumnMetadata> columnMetadataList = null;
        TableMetadataStore tableMetadataStore = null;
        ColumnMetadataStore columnStore = null;
        try {
            tableMetadataStore = new TableMetadataStore(databaseId);
            columnStore = new ColumnMetadataStore(databaseId);
            tableMetadata = tableMetadataStore.getTable(tableName);
            if(tableMetadata == null) {
                throw new SqlExecutionException("表" + tableName + "不存在");
            }
            TableColumnBlock columnBlock = columnStore.getColumnBlock(tableMetadata.getTableId());
            columnMetadataList = columnBlock.getColumnMetadataList();
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DbException("获取字段信息发生异常");
        } finally {
            if(columnStore != null) {
                columnStore.close();
            }
            if(tableMetadataStore != null) {
                tableMetadataStore.close();
            }
        }

        if(CollectionUtils.isEmpty(columnMetadataList)) {
            ExceptionUtil.throwDbException("表{}缺少字段", tableName);
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

        this.columns = columns;
        this.engineType = tableMetadata.getEngineType();
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.tableId = tableMetadata.getTableId();
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public String getEngineType() {
        return engineType;
    }
}
