package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.util.PathUtil;
/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class TruncateTableCommand extends AbstractCommand {


    private static final String FILE_PATH = PathUtil.getBaseDirPath();

    private String tableName;

    public TruncateTableCommand(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String execute() {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = FILE_PATH + tableName + ".d";
            dataChunkStore = new DataChunkStore(fileFullPath);
            dataChunkStore.truncateTable();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }

}
