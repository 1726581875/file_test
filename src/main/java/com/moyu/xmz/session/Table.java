package com.moyu.xmz.session;

import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMeta;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.meta.TableMeta;
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


    public Table(TableMeta tableMeta) {
        this.tableId = tableMeta.getTableId();
        this.tableName = tableMeta.getTableName();
        this.databaseId = tableMeta.getDatabaseId();
        this.engineType = tableMeta.getEngineType();
    }

    public Table(Integer databaseId, String tableName) {
        init(databaseId, tableName);
    }

    public String getTableName() {
        return tableName;
    }


    public Column[] getColumns(boolean refresh) {
        if(refresh) {
            init(databaseId, tableName);
        } else {
            initIfNon();
        }
        return columns;
    }

    public Column[] getColumns() {
        initIfNon();
        return columns;
    }


    public Column getColumn(String columnName) {
        initIfNon();
        for (Column column : this.columns) {
            if(column.getColumnName().equals(columnName)) {
                return column;
            }
        }
        throw ExceptionUtil.buildDbException("表{}获取字段信息发生异常,字段{}不存在", this.tableName, columnName);
    }

    private void initIfNon() {
        if(columns == null || columns.length == 0) {
            init(databaseId, tableName);
        }
    }


    private void init(Integer databaseId, String tableName) {

        TableMeta tableMeta = null;
        List<ColumnMeta> columnMetaList = null;
        TableMetaAccessor tableMetaAccessor = null;
        ColumnMetaAccessor columnStore = null;
        try {
            tableMetaAccessor = new TableMetaAccessor(databaseId);
            columnStore = new ColumnMetaAccessor(databaseId);
            tableMeta = tableMetaAccessor.getTable(tableName);
            if(tableMeta == null) {
                ExceptionUtil.throwSqlExecutionException("数据库id{}的表{}不存在", databaseId, tableName);
            }
            TableColumnBlock columnBlock = columnStore.getColumnBlock(tableMeta.getTableId());
            columnMetaList = columnBlock.getColumnMetaList();
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            ExceptionUtil.throwDbException("数据库id{}的表{}获取字段信息发生异常", databaseId, tableName);
        } finally {
            if(columnStore != null) {
                columnStore.close();
            }
            if(tableMetaAccessor != null) {
                tableMetaAccessor.close();
            }
        }

        if(CollectionUtils.isEmpty(columnMetaList)) {
            ExceptionUtil.throwDbException("表{}缺少字段", tableName);
        }

        Column[] columns = new Column[columnMetaList.size()];
        for (int i = 0; i < columnMetaList.size(); i++) {
            columns[i] = new Column(columnMetaList.get(i));
        }

        this.columns = columns;
        this.engineType = tableMeta.getEngineType();
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.tableId = tableMeta.getTableId();
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
