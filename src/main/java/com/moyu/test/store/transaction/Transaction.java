package com.moyu.test.store.transaction;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.SerializableByte;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.util.DataUtils;
import com.moyu.test.util.PathUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class Transaction implements TxOperator, SerializableByte {

    public static final int BLOCK_SIZE = 4096;

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
    /**
     * 回滚失败
     */
    public static final int STATUS_ROLLBACK_FAILED = 5;


    /**
     * 占用字节长度
     */
    private int totalByteLen;
    /**
     * 开始位置
     */
    private long startPos;
    /**
     * 事务id
     */
    private int transactionId;
    /**
     * 事务状态
     */
    private int status;
    /**
     * 事务开始时间
     */
    private long startTime;

    /**
     * 修改记录数
     */
    private int recordNum;
    /**
     * 事务包含的数据修改记录
     */
    private List<RowLogRecord> rowLogRecords;


    public Transaction(int transactionId, int status, long startTime) {
        this.transactionId = transactionId;
        this.startTime = startTime;
        this.status = status;
        this.rowLogRecords = new ArrayList<>();
    }


    public Transaction(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.transactionId = DataUtils.readInt(byteBuffer);
        this.status = DataUtils.readInt(byteBuffer);
        this.startTime = DataUtils.readLong(byteBuffer);
        this.recordNum = DataUtils.readInt(byteBuffer);
        this.rowLogRecords = new ArrayList<>();
        for (int i = 0; i < this.recordNum; i++) {
            this.rowLogRecords.add(new RowLogRecord(byteBuffer));
        }
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BLOCK_SIZE);
        DataUtils.writeInt(byteBuffer, this.totalByteLen);
        DataUtils.writeLong(byteBuffer, this.startPos);
        DataUtils.writeInt(byteBuffer, this.transactionId);
        DataUtils.writeInt(byteBuffer, this.status);
        DataUtils.writeLong(byteBuffer, this.startTime);

        this.recordNum = rowLogRecords.size();
        DataUtils.writeInt(byteBuffer, this.recordNum);
        for (int i = 0; i < this.rowLogRecords.size(); i++) {
            RowLogRecord rowLogRecord = this.rowLogRecords.get(i);
            byteBuffer.put(rowLogRecord.getByteBuffer());
        }
        // 获取真实长度
        this.totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, this.totalByteLen);
        byteBuffer.rewind();
        return byteBuffer;
    }


    public void addRowLogRecord(RowLogRecord record) {
        this.rowLogRecords.add(record);
    }


    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
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

    public int getTotalByteLen() {
        return totalByteLen;
    }

    public void setTotalByteLen(int totalByteLen) {
        this.totalByteLen = totalByteLen;
    }

    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public int getRecordNum() {
        return recordNum;
    }

    public void setRecordNum(int recordNum) {
        this.recordNum = recordNum;
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
        boolean success = false;
        int failNum = 0;
        do {
            for (int i = rowLogRecords.size() - 1; i >= 0; i--) {
                RowLogRecord record = rowLogRecords.get(i);
                DataChunkStore dataChunkStore = null;
                try {
                    dataChunkStore = new DataChunkStore(PathUtil.getDataFilePath(record.getDatabaseId(), record.getTableName()));
                    // 找到行所在数据块
                    DataChunk chunk = dataChunkStore.getChunkByPos(record.getBlockPos());
                    // 找到对应行
                    int index = findRow(chunk.getDataRowList(), record.getRowId());
                    // 更新对应行记录为旧值
                    if (index == -1) {
                        throw new DbException("回滚失败，数据不存在");
                    }
                    chunk.updateRow(index, record.getOldRow());
                    dataChunkStore.updateChunk(chunk);
                    success = true;
                    this.status = STATUS_ROLLED_BACK;
                } catch (Exception e) {
                    failNum++;
                    e.printStackTrace();
                } finally {
                    dataChunkStore.close();
                }
            }
        } while (!success && failNum > 5);

        this.status = STATUS_ROLLBACK_FAILED;
    }



    private static int findRow(List<RowData> dataRowList, long rowId) {
        for (int j = 0; j < dataRowList.size(); j++) {
            RowData rowData = dataRowList.get(j);
            if (rowData.getRowId() == rowId) {
                return j;
            }
        }
        return -1;
    }


    @Override
    public String toString() {
        return "Transaction{" +
                "totalByteLen=" + totalByteLen +
                ", startPos=" + startPos +
                ", transactionId=" + transactionId +
                ", status=" + status +
                ", startTime=" + startTime +
                ", recordNum=" + recordNum +
                ", rowLogRecords=" + rowLogRecords +
                '}';
    }
}
