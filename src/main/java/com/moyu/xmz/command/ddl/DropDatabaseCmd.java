package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.accessor.DatabaseMetaAccessor;
import com.moyu.xmz.store.common.meta.DatabaseMeta;
import com.moyu.xmz.store.common.meta.TableMeta;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class DropDatabaseCmd extends AbstractCmd {

    private String databaseName;

    private boolean isExists;

    public DropDatabaseCmd() {
    }

    public DropDatabaseCmd(String databaseName, boolean isExists) {
        this.databaseName = databaseName;
        this.isExists = isExists;
    }

    @Override
    public QueryResult exec() {
        boolean isSuccess = true;
        DatabaseMetaAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaAccessor();
            DatabaseMeta metadata = metadataStore.getDatabase(databaseName);
            if (metadata == null && isExists) {
                return QueryResult.simpleResult(RESULT_OK);
            }
            Database database = Database.getDatabase(metadata.getDatabaseId());
            metadataStore.dropDatabase(databaseName);
            // 删除所有表
            ShowTablesCmd showTablesCmd = new ShowTablesCmd(metadata.getDatabaseId());
            showTablesCmd.exec();
            List<TableMeta> resultList = showTablesCmd.getResultList();
            for (TableMeta tableMeta : resultList) {
                DropTableCmd dropTableCmd = new DropTableCmd(database, tableMeta.getTableName(), true);
                dropTableCmd.exec();
            }
            Database.removeDatabase(metadata.getDatabaseId());

        } catch (Exception e) {
            e.printStackTrace();
            isSuccess = false;
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        return isSuccess ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setExists(boolean exists) {
        isExists = exists;
    }
}
