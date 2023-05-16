package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class InsertCommand extends AbstractCommand {

    // TODO 有时间统一基础路径
    private static final String filePath = "D:\\mytest\\fileTest\\";

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
            String fileFullPath = filePath + tableName + ".d";
            dataChunkStore = new DataChunkStore(fileFullPath);
            dataChunkStore.storeRow(columns);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return null;
    }

}
