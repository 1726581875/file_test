package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.accessor.DatabaseMetaFileAccessor;
import com.moyu.xmz.store.common.meta.DatabaseMetadata;

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
    public QueryResult execCommand() {
        boolean success = true;
        DatabaseMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaFileAccessor();
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
        return success ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }
}
