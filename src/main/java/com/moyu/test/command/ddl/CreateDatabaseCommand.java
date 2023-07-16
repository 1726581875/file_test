package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class CreateDatabaseCommand extends AbstractCommand {

    private String databaseName;

    public CreateDatabaseCommand(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public String execute() {

        boolean success = true;
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore();
            DatabaseMetadata metadata = metadataStore.createDatabase(databaseName);
            Database.putDatabase(metadata.getDatabaseId(), new Database(metadata.getDatabaseId(), metadata.getName()));
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        return success ? "ok" : "error";
    }
}
