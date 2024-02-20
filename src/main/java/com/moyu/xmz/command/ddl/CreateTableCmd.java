package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;
import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.TableInfo;
import com.moyu.xmz.store.common.meta.TableMeta;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class CreateTableCmd extends AbstractCmd {

    private ConnectSession session;

    private Integer databaseId;

    private String tableName;

    private String engineType;

    private List<Column> columnList;

    @Override
    public QueryResult exec() {
        boolean isSuccess = true;
        TableMetaAccessor tableAcc = null;
        ColumnMetaAccessor columnAcc = null;
        IndexMetaAccessor indexAcc = null;
        try {
            tableAcc = new TableMetaAccessor(databaseId);
            columnAcc = new ColumnMetaAccessor(databaseId);
            indexAcc = new IndexMetaAccessor(databaseId);
            // 插入table元数据
            TableMeta table = tableAcc.createTable(tableName, engineType);
            // 插入column元数据
            columnAcc.createColumnBlock(table.getTableId(), columnList);
            // 添加该表信息到内存
            Table tableObj = new Table(table);
            session.getDatabase().addTable(tableObj);
            // 如果有主键，创建主键索引
            Column keyColumn = getPrimaryKeyColumn(columnList.toArray(new Column[0]));
            if(keyColumn != null && CommonConstant.ENGINE_TYPE_YU.equals(engineType)) {
                TableInfo tableInfo = new TableInfo(session, tableObj, null);
                CreateIndexCmd indexCmd = new CreateIndexCmd(tableInfo);
                indexCmd.setIndexName(keyColumn.getColumnName());
                indexCmd.setColumnName(keyColumn.getColumnName());
                indexCmd.setIndexType(CommonConstant.PRIMARY_KEY);
                indexCmd.exec();
            }
        } catch (Exception e) {
            isSuccess = false;
            e.printStackTrace();
        } finally {
            if (tableAcc != null) {
                tableAcc.close();
            }
            if (columnAcc != null) {
                columnAcc.close();
            }
            if (indexAcc != null) {
                indexAcc.close();
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
