package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;
import com.moyu.test.store.metadata.obj.TableMetadata;
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
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore();
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
