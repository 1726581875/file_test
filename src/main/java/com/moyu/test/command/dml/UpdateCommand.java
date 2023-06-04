package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.condition.ConditionTree;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.PathUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/20
 */
public class UpdateCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    private Column[] updateColumns;

    private Column[] columns;

    private ConditionTree conditionTree;


    public UpdateCommand(Integer databaseId,
                         String tableName,
                         Column[] columns,
                         Column[] updateColumns,
                         ConditionTree conditionTree) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.columns = columns;
        this.updateColumns = updateColumns;
        this.conditionTree = conditionTree;
    }

    @Override
    public String execute() {
        int updateRowNum = 0;
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
            // 遍历数据块
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
                    if (conditionTree == null) {
                        updateColumnData(columnData);
                        RowData newRow = new RowData(rowData.getStartPos(), RowData.toRowByteData(columnData));
                        chunk.updateRow(j, newRow);
                        updateRowNum++;
                    } else {
                        boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
                        if (compareResult) {
                            updateColumnData(columnData);
                            RowData newRow = new RowData(rowData.getStartPos(), RowData.toRowByteData(columnData));
                            chunk.updateRow(j, newRow);
                            updateRowNum++;
                        }
                    }
                }
                // 更新块整个数据块到磁盘
                dataChunkStore.updateChunk(chunk);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new SqlExecutionException("执行更新语句发生异常");
        } finally {
            dataChunkStore.close();
        }
        return "共更新了" + updateRowNum + "行数据";
    }


    private void updateColumnData(Column[] columns) {
        for (Column newValueColumn : updateColumns) {
            columns[newValueColumn.getColumnIndex()].setValue(newValueColumn.getValue());
        }
    }


}
