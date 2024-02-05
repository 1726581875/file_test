package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.accessor.DatabaseMetaAccessor;
import com.moyu.xmz.store.common.meta.DatabaseMeta;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class CreateDatabaseCmd extends AbstractCmd {

    private String databaseName;

    public CreateDatabaseCmd(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public QueryResult execCommand() {
        boolean success = true;
        DatabaseMetaAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaAccessor();
            DatabaseMeta metadata = metadataStore.createDatabase(databaseName);
            Database.putDatabase(metadata.getDatabaseId(), new Database(metadata.getDatabaseId(), metadata.getName()));
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        return success ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }
}
