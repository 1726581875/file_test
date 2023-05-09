package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.DatabaseMetadataStore;

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
            metadataStore.dropDatabase(databaseName);
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
