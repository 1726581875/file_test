package com.moyu.test.store.transaction;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.util.PathUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class Transaction implements TxOperator {

    /**
     * 事务进行中
     */
    public static final int STATUS_ACTIVITY = 1;
    /**
     * 事务已提交
     */
    public static final int STATUS_COMMITTED = 2;
    /**
     * 事务回滚中
     */
    public static final int STATUS_ROLLING_BACK = 3;

    /**
     * 事务已回滚
     */
    public static final int STATUS_ROLLED_BACK = 4;


    private int id;

    private int status;

    private long startTime;

    private List<RowLogRecord> rowLogRecords;

    public Transaction(int id, int status, long startTime) {
        this.id = id;
        this.startTime = startTime;
        this.status = status;
        this.rowLogRecords = new ArrayList<>();
    }


    public void addRowLogRecord(RowLogRecord record) {
        this.rowLogRecords.add(record);
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public List<RowLogRecord> getRowLogRecords() {
        return rowLogRecords;
    }

    public void setRowLogRecords(List<RowLogRecord> rowLogRecords) {
        this.rowLogRecords = rowLogRecords;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    @Override
    public void begin() {

    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {
        this.status = STATUS_ROLLING_BACK;
        for (int i = rowLogRecords.size() - 1; i >= 0; i--) {
            RowLogRecord record = rowLogRecords.get(i);
            DataChunkStore dataChunkStore = null;
            try {
                dataChunkStore = new DataChunkStore(PathUtil.getDataFilePath(record.getDatabaseId(), record.getTableName()));
                // 找到行所在数据块
                DataChunk chunk = dataChunkStore.getChunkByPos(record.getBlockPos());
                // 找到对应行
                int index = -1;
                List<RowData> dataRowList = chunk.getDataRowList();
                for (int j = 0; j < dataRowList.size(); j++) {
                    RowData rowData = dataRowList.get(j);
                    if (rowData.getRowId() == record.getRowId()) {
                        index = j;
                    }
                }
                // 更新对应行为旧值
                if (index == -1) {
                    throw new DbException("回滚失败，数据不存在");
                }
                chunk.updateRow(index, record.getOldRow());
                dataChunkStore.updateChunk(chunk);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                dataChunkStore.close();
            }
        }
    }
}
