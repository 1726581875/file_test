package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.util.PathUtil;
/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class TruncateTableCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    public TruncateTableCommand(Integer databaseId, String tableName) {
        this.databaseId = databaseId;
        this.tableName = tableName;
    }

    @Override
    public QueryResult execCommand() {
        boolean isSuccess = false;
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            dataChunkStore.truncateTable();
            isSuccess = true;
        } catch (Exception e) {
            isSuccess = false;
            e.printStackTrace();
        } finally {
            if(dataChunkStore != null) {
                dataChunkStore.close();
            }
        }
        return isSuccess ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }

}
