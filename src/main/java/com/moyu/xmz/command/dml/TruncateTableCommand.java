package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.accessor.DataChunkFileAccessor;
import com.moyu.xmz.common.util.PathUtil;

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
        DataChunkFileAccessor dataChunkFileAccessor = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkFileAccessor = new DataChunkFileAccessor(fileFullPath);
            dataChunkFileAccessor.truncateTable();
            isSuccess = true;
        } catch (Exception e) {
            isSuccess = false;
            e.printStackTrace();
        } finally {
            if(dataChunkFileAccessor != null) {
                dataChunkFileAccessor.close();
            }
        }
        return isSuccess ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }

}
