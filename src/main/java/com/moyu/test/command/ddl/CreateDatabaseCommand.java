package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.DatabaseMetadataStore;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class CreateDatabaseCommand extends AbstractCommand {

    private String databaseName;

    public void execute() {
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore();
            metadataStore.createDatabase(databaseName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
