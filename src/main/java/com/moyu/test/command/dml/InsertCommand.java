package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.PathUtil;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class InsertCommand extends AbstractCommand {

    private static final String FILE_PATH = PathUtil.getBaseDirPath();

    private String tableName;

    private Column[] columns;

    public InsertCommand(String tableName, Column[] columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public String execute() {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = FILE_PATH + tableName + ".d";
            dataChunkStore = new DataChunkStore(fileFullPath);
            dataChunkStore.storeRow(columns);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }

}
