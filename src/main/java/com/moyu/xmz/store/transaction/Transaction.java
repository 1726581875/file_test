package com.moyu.xmz.store.transaction;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.SerializableByte;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.accessor.DataChunkAccessor;
import com.moyu.xmz.store.common.meta.RowMeta;
import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.common.util.PathUtils;

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
        this.totalByteLen = DataByteUtils.readInt(byteBuffer);
        this.startPos = DataByteUtils.readLong(byteBuffer);
        this.transactionId = DataByteUtils.readInt(byteBuffer);
        this.status = DataByteUtils.readInt(byteBuffer);
        this.startTime = DataByteUtils.readLong(byteBuffer);
        this.recordNum = DataByteUtils.readInt(byteBuffer);
        this.rowLogRecords = new ArrayList<>();
        for (int i = 0; i < this.recordNum; i++) {
            this.rowLogRecords.add(new RowLogRecord(byteBuffer));
        }
    }


    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BLOCK_SIZE);
        DataByteUtils.writeInt(byteBuffer, this.totalByteLen);
        DataByteUtils.writeLong(byteBuffer, this.startPos);
        DataByteUtils.writeInt(byteBuffer, this.transactionId);
        DataByteUtils.writeInt(byteBuffer, this.status);
        DataByteUtils.writeLong(byteBuffer, this.startTime);

        this.recordNum = rowLogRecords.size();
        DataByteUtils.writeInt(byteBuffer, this.recordNum);
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
                DataChunkAccessor dataChunkAccessor = null;
                try {
                    dataChunkAccessor = new DataChunkAccessor(PathUtils.getDataFilePath(record.getDatabaseId(), record.getTableName()));
                    // 找到行所在数据块
                    DataChunk chunk = record.getBlockPos() > 0 ? dataChunkAccessor.getChunkByPos(record.getBlockPos())
                            : getRowDataChunk(dataChunkAccessor, record.getRowId());
                    // 找到对应行
                    int index = findRow(chunk, record.getRowId());
                    // 回滚insert操作
                    if(RowLogRecord.TYPE_INSERT == record.getType()) {
                        if(index != -1) {
                            chunk.removeRow(index);
                            dataChunkAccessor.updateChunk(chunk);
                        }
                    } else if /* 回滚update操作 */ (RowLogRecord.TYPE_UPDATE == record.getType()) {
                        if (index == -1) {
                            throw new DbException("回滚失败，数据不存在");
                        }
                        chunk.updateRow(index, record.getOldRow());
                        dataChunkAccessor.updateChunk(chunk);
                    } else if /* 回滚delete操作 */(RowLogRecord.TYPE_DELETE == record.getType()) {
                        // 数据存在，替换为旧数据
                        if (index != -1) {
                            chunk.updateRow(index, record.getOldRow());
                        } else {
                        // 数据不存在，新增
                            chunk.addRow(record.getOldRow());
                        }
                        dataChunkAccessor.updateChunk(chunk);

                    }
                    success = true;
                    this.status = STATUS_ROLLED_BACK;
                    TransactionManager.recordTransaction(this);
                } catch (Exception e) {
                    failNum++;
                    e.printStackTrace();
                } finally {
                    dataChunkAccessor.close();
                }
            }
        } while (!success && failNum > 5);

        this.status = STATUS_ROLLBACK_FAILED;
    }


    /**
     * 获取数据行所在的数据块
     * @param dataChunkAccessor
     * @param rowId
     * @return
     */
    private static DataChunk getRowDataChunk(DataChunkAccessor dataChunkAccessor, long rowId) {
        int dataChunkNum = dataChunkAccessor.getDataChunkNum();
        for (int i = 1; i <=dataChunkNum; i++) {
            DataChunk chunk = dataChunkAccessor.getChunk(i);
            if(chunk == null) {
                continue;
            }
            int rowIndex = findRow(chunk, rowId);
            if(rowIndex > 0) {
                return chunk;
            }
        }
        return null;
    }


    private static int findRow(DataChunk chunk, long rowId) {

        if(chunk == null)  {
            return -1;
        }

        if (rowId < 0) {
            return -1;
        }

        for (int j = 0; j < chunk.getDataRowList().size(); j++) {
            RowMeta rowMeta = chunk.getDataRowList().get(j);
            if (rowMeta.getRowId() == rowId) {
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
