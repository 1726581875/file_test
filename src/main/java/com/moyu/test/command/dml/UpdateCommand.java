package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.dml.condition.ConditionComparator;
import com.moyu.test.command.dml.sql.ConditionTree2;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.transaction.RowLogRecord;
import com.moyu.test.store.transaction.Transaction;
import com.moyu.test.store.transaction.TransactionManager;
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

    private ConditionTree2 conditionTree;

    private ConnectSession session;


    public UpdateCommand(Integer databaseId,
                         String tableName,
                         Column[] columns,
                         Column[] updateColumns,
                         ConditionTree2 conditionTree) {
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

                for (int j = 0; j < dataRowList.size(); j++) {
                    RowData rowData = dataRowList.get(j);
                    Column[] columnData = rowData.getColumnData(columns);

                    boolean match = ConditionComparator.isMatch(new RowEntity(columnData), conditionTree);
                    if (match) {

                        Transaction transaction = TransactionManager.getTransaction(session.getTransactionId());
                        // 记录到undo log
                        if(transaction != null) {
                            RowLogRecord record = new RowLogRecord();
                            record.setBlockPos(chunk.getStartPos());
                            record.setDatabaseId(this.databaseId);
                            record.setTableName(this.tableName);
                            record.setRowId(rowData.getRowId());
                            record.setOldRow(rowData);
                            record.setTransactionId(transaction.getId());
                            transaction.addRowLogRecord(record);
                        }

                        updateColumnData(columnData);
                        RowData newRow = new RowData(rowData.getStartPos(), RowData.toRowByteData(columnData), rowData.getRowId());
                        chunk.updateRow(j, newRow);
                        updateRowNum++;
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


    public void setSession(ConnectSession session) {
        this.session = session;
    }
}
