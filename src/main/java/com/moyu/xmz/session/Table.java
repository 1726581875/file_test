package com.moyu.xmz.session;

import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.store.accessor.ColumnMetaFileAccessor;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMetadata;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.meta.TableMetadata;
import com.moyu.xmz.common.util.CollectionUtils;

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
        TableMetaFileAccessor tableMetaFileAccessor = null;
        ColumnMetaFileAccessor columnStore = null;
        try {
            tableMetaFileAccessor = new TableMetaFileAccessor(databaseId);
            columnStore = new ColumnMetaFileAccessor(databaseId);
            tableMetadata = tableMetaFileAccessor.getTable(tableName);
            if(tableMetadata == null) {
                ExceptionUtil.throwSqlExecutionException("数据库id{}的表{}不存在", databaseId, tableName);
            }
            TableColumnBlock columnBlock = columnStore.getColumnBlock(tableMetadata.getTableId());
            columnMetadataList = columnBlock.getColumnMetadataList();
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            ExceptionUtil.throwDbException("数据库id{}的表{}获取字段信息发生异常", databaseId, tableName);
        } finally {
            if(columnStore != null) {
                columnStore.close();
            }
            if(tableMetaFileAccessor != null) {
                tableMetaFileAccessor.close();
            }
        }

        if(CollectionUtils.isEmpty(columnMetadataList)) {
            ExceptionUtil.throwDbException("表{}缺少字段", tableName);
        }

        Column[] columns = new Column[columnMetadataList.size()];
        for (int i = 0; i < columnMetadataList.size(); i++) {
            columns[i] = new Column(columnMetadataList.get(i));
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

    public Integer getTableId() {
        return tableId;
    }
}
