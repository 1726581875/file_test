package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.PathUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class SelectCommand extends AbstractCommand {

    private static final String FILE_PATH = PathUtil.getBaseDirPath();

    private String tableName;

    private Column[] columns;


    public SelectCommand(String tableName, Column[] columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public String execute() {

        StringBuilder stringBuilder = new StringBuilder();

        String tableHeaderStr = "";
        for (Column column : columns) {
            String value = column.getColumnName();
            tableHeaderStr = tableHeaderStr + " | " + value;
        }
        stringBuilder.append(tableHeaderStr + " | " + "\n");
        stringBuilder.append("--------------" + "\n");

        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = FILE_PATH + tableName + ".d";
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
            for (int i = 0; i < dataChunkNum; i++) {
                DataChunk chunk = dataChunkStore.getChunk(i);
                if (chunk == null) {
                    break;
                }
                List<RowData> dataRowList = chunk.getDataRowList();
                if (dataRowList == null || dataRowList.size() == 0) {
                    continue;
                }
                for (int j = 0; j < dataRowList.size(); j++) {
                    RowData rowData = dataRowList.get(j);
                    Column[] columnData = rowData.getColumnData(columns);
                    String resultStr = "";
                    for (Column column : columnData) {
                        Object value = column.getValue();
                        resultStr = resultStr + " | " + value;
                    }
                    stringBuilder.append(resultStr + " | " + "\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return stringBuilder.toString();
    }
}
