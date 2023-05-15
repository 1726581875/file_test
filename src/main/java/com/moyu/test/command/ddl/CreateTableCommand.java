package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.TableMetadata;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class CreateTableCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    private List<Column> columnDtoList;

    @Override
    public String execute() {
        boolean isSuccess = true;
        TableMetadataStore tableMetadataStore = null;
        ColumnMetadataStore columnMetadataStore = null;
        try {
            tableMetadataStore = new TableMetadataStore(databaseId);
            columnMetadataStore = new ColumnMetadataStore();
            TableMetadata table = tableMetadataStore.createTable(tableName);
            columnMetadataStore.createColumnBlock(table.getTableId(), columnDtoList);
        } catch (Exception e) {
            isSuccess = false;
            e.printStackTrace();
        } finally {
            if (tableMetadataStore != null) {
                tableMetadataStore.close();
            }
            if (columnMetadataStore != null) {
                columnMetadataStore.close();
            }
        }
        return isSuccess ? "ok" : "error";
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

    public List<Column> getColumnDtoList() {
        return columnDtoList;
    }

    public void setColumnDtoList(List<Column> columnDtoList) {
        this.columnDtoList = columnDtoList;
    }
}
