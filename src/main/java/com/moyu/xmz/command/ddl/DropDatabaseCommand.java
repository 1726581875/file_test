package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.accessor.DatabaseMetaFileAccessor;
import com.moyu.xmz.store.common.meta.DatabaseMetadata;
import com.moyu.xmz.store.common.meta.TableMetadata;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class DropDatabaseCommand extends AbstractCommand {

    private String databaseName;

    private boolean isExists;

    public DropDatabaseCommand() {
    }

    public DropDatabaseCommand(String databaseName, boolean isExists) {
        this.databaseName = databaseName;
        this.isExists = isExists;
    }

    @Override
    public QueryResult execCommand() {
        boolean isSuccess = true;
        DatabaseMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaFileAccessor();
            DatabaseMetadata metadata = metadataStore.getDatabase(databaseName);
            if (metadata == null && isExists) {
                return QueryResult.simpleResult(RESULT_OK);
            }
            Database database = Database.getDatabase(metadata.getDatabaseId());
            metadataStore.dropDatabase(databaseName);
            // 删除所有表
            ShowTablesCommand showTablesCommand = new ShowTablesCommand(metadata.getDatabaseId());
            List<TableMetadata> resultList = showTablesCommand.getResultList();
            for (TableMetadata tableMetadata : resultList) {
                DropTableCommand dropTableCommand = new DropTableCommand(database, tableMetadata.getTableName(), true);
                dropTableCommand.execCommand();
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
