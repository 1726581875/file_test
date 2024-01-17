package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.accessor.ColumnMetaFileAccessor;
import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.TableMetadata;
import com.moyu.xmz.store.common.dto.OperateTableInfo;

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
    public QueryResult execCommand() {
        boolean isSuccess = true;
        TableMetaFileAccessor tableMetaFileAccessor = null;
        ColumnMetaFileAccessor columnMetaFileAccessor = null;
        IndexMetaFileAccessor indexMetaFileAccessor = null;
        try {
            tableMetaFileAccessor = new TableMetaFileAccessor(databaseId);
            columnMetaFileAccessor = new ColumnMetaFileAccessor(databaseId);
            indexMetaFileAccessor = new IndexMetaFileAccessor(databaseId);
            // 插入table元数据
            TableMetadata table = tableMetaFileAccessor.createTable(tableName, engineType);
            // 插入column元数据
            columnMetaFileAccessor.createColumnBlock(table.getTableId(), columnList);
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
                indexCommand.execCommand();
            }

            Database database = session.getDatabase();
            database.addTable(new Table(table));
        } catch (Exception e) {
            isSuccess = false;
            e.printStackTrace();
        } finally {
            if (tableMetaFileAccessor != null) {
                tableMetaFileAccessor.close();
            }
            if (columnMetaFileAccessor != null) {
                columnMetaFileAccessor.close();
            }
            if (indexMetaFileAccessor != null) {
                indexMetaFileAccessor.close();
            }
        }
        return isSuccess ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
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
