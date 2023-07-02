package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.operation.BasicOperation;
import com.moyu.test.store.operation.OperateTableInfo;
import com.moyu.test.util.PathUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class InsertCommand extends AbstractCommand {

    private OperateTableInfo tableInfo;

    private Column[] dataColumns;


    public InsertCommand(OperateTableInfo tableInfo, Column[] dataColumns) {
        this.tableInfo = tableInfo;
        this.dataColumns = dataColumns;
    }

    @Override
    public String execute() {
        BasicOperation engineOperation = BasicOperation.getEngineOperation(tableInfo);
        int num = engineOperation.insert(new RowEntity(dataColumns));
        return num == 1 ? "ok" : "error";
    }




    public String batchWriteRows(List<RowEntity> columnsList) {
        List<Column[]> columnList = new ArrayList<>(columnsList.size());
        for (RowEntity row : columnsList) {
            columnList.add(row.getColumns());
        }
        return batchWriteList(columnList);
    }

    public String batchWriteList(List<Column[]> columnsList) {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(tableInfo.getSession().getDatabaseId(), tableInfo.getTableName());
            dataChunkStore = new DataChunkStore(fileFullPath);
            List<byte[]> list = new ArrayList<>();

            for (int i = 0; i < columnsList.size(); i++) {
                Column[] columns = columnsList.get(i);
                byte[] rowBytes = RowData.toRowByteData(columns);
                list.add(rowBytes);
            }
            dataChunkStore.writeRow(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }


    public Column[] getDataColumns() {
        return dataColumns;
    }
}
