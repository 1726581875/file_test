package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.session.Table;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.TableMetadata;
import com.moyu.test.store.operation.OperateTableInfo;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class CreateTableCommand extends AbstractCommand {

    private ConnectSession session;

    private Integer databaseId;

    private String tableName;

    private String engineType;

    private List<Column> columnList;

    @Override
    public String execute() {
        boolean isSuccess = true;
        TableMetadataStore tableMetadataStore = null;
        ColumnMetadataStore columnMetadataStore = null;
        IndexMetadataStore indexMetadataStore = null;
        try {
            tableMetadataStore = new TableMetadataStore(databaseId);
            columnMetadataStore = new ColumnMetadataStore(databaseId);
            indexMetadataStore = new IndexMetadataStore(databaseId);
            // 插入table元数据
            TableMetadata table = tableMetadataStore.createTable(tableName, engineType);
            // 插入column元数据
            columnMetadataStore.createColumnBlock(table.getTableId(), columnList);
            // 如果有主键，创建主键索引
            Column keyColumn = getPrimaryKeyColumn(columnList.toArray(new Column[0]));
            if(keyColumn != null && CommonConstant.ENGINE_TYPE_YU.equals(engineType)) {
                OperateTableInfo tableInfo = new OperateTableInfo(session, tableName, columnList.toArray(new Column[0]), null);
                CreateIndexCommand indexCommand = new CreateIndexCommand(tableInfo);
                indexCommand.setDatabaseId(databaseId);
                indexCommand.setTableId(table.getTableId());
                indexCommand.setColumnName(keyColumn.getColumnName());
                indexCommand.setIndexName(keyColumn.getColumnName());
                indexCommand.setTableName(tableName);
                indexCommand.setColumns(columnList.toArray(new Column[0]));
                indexCommand.setIndexColumn(keyColumn);
                indexCommand.setIndexType(CommonConstant.PRIMARY_KEY);
                indexCommand.execute();
            }

            Database database = session.getDatabase();
            database.addTable(new Table(table));
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
            if (indexMetadataStore != null) {
                indexMetadataStore.close();
            }
        }
        return isSuccess ? "ok" : "error";
    }


    private Column getPrimaryKeyColumn(Column[] columnArr) {
        for (Column c : columnArr) {
            if (c.getIsPrimaryKey() == (byte) 1) {
                return c;
            }
        }
        return null;
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

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }


    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }


    public void setSession(ConnectSession session) {
        this.session = session;
    }


    @Override
    public String toString() {
        return "CreateTableCommand{" +
                "databaseId=" + databaseId +
                ", tableName='" + tableName + '\'' +
                ", columnList=" + columnList +
                '}';
    }
}
