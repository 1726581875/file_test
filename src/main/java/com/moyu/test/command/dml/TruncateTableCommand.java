package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
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
    public String execute() {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            dataChunkStore.truncateTable();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(dataChunkStore != null) {
                dataChunkStore.close();
            }
        }
        return "ok";
    }

}
