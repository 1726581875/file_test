package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.accessor.DataChunkAccessor;
import com.moyu.xmz.common.util.PathUtils;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class TruncateTableCmd extends AbstractCmd {

    private Integer databaseId;

    private String tableName;

    public TruncateTableCmd(Integer databaseId, String tableName) {
        this.databaseId = databaseId;
        this.tableName = tableName;
    }

    @Override
    public QueryResult execCommand() {
        boolean isSuccess = false;
        DataChunkAccessor dataChunkAccessor = null;
        try {
            String fileFullPath = PathUtils.getDataFilePath(this.databaseId, this.tableName);
            dataChunkAccessor = new DataChunkAccessor(fileFullPath);
            dataChunkAccessor.truncateTable();
            isSuccess = true;
        } catch (Exception e) {
            isSuccess = false;
            e.printStackTrace();
        } finally {
            if(dataChunkAccessor != null) {
                dataChunkAccessor.close();
            }
        }
        return isSuccess ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }

}
