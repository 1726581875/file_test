package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;

/**
 * @author xiaomingzhang
 * @date 2023/5/9
 */
public class DropDatabaseCommand extends AbstractCommand {

    private String databaseName;

    @Override
    public String execute() {
        boolean isSuccess = true;
        DatabaseMetadataStore metadataStore = null;

        try {
            metadataStore = new DatabaseMetadataStore();
            DatabaseMetadata metadata = metadataStore.getDatabase(databaseName);
            Database database = Database.getDatabase(metadata.getDatabaseId());
            metadataStore.dropDatabase(databaseName);
            // 删除所有表
            ShowTablesCommand showTablesCommand = new ShowTablesCommand(metadata.getDatabaseId());
            String[] allTable = showTablesCommand.getAllTable();
            for (String tableName : allTable) {
                DropTableCommand dropTableCommand = new DropTableCommand(database, tableName, true);
                dropTableCommand.execute();
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
        return isSuccess ? "ok" : "error";
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
