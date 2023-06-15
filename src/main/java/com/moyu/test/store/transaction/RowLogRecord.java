package com.moyu.test.store.transaction;

import com.moyu.test.store.SerializableByte;
import com.moyu.test.store.data.RowData;
import com.moyu.test.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class RowLogRecord implements SerializableByte {

    public static final byte TYPE_INSERT = 0;
    public static final byte TYPE_UPDATE = 1;
    public static final byte TYPE_DELETE = 2;

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
     * 数据库id
     */
    private int databaseId;
    /**
     * 所属表名
     */
    private String tableName;
    /**
     * 数据块位置
     */
    private long blockPos;
    /**
     * 数据行id
     */
    private long rowId;
    /**
     * 版本
     */
    private int version;

    /**
     * 操作类型
     * 0新增、1修改、2删除
     */
    private byte type;

    /**
     * 旧数据
     */
    private RowData oldRow;


    public RowLogRecord(String tableName, RowData oldRow, byte type) {
        this.tableName = tableName;
        this.oldRow = oldRow;
        this.type = type;
        int rowLen = this.oldRow == null ? 0 : (int)this.oldRow.getTotalByteLen();
        // tableName + oldRow length + 4 int + 3 long + 1 byte
        this.totalByteLen = (4 + this.tableName.length() * 3) + rowLen + (4 * 4) + (3 * 8) + 1;
    }

    public RowLogRecord(ByteBuffer byteBuffer) {
        this.totalByteLen = DataUtils.readInt(byteBuffer);
        this.startPos = DataUtils.readLong(byteBuffer);
        this.transactionId = DataUtils.readInt(byteBuffer);
        this.databaseId = DataUtils.readInt(byteBuffer);
        int tableNameLen = DataUtils.readInt(byteBuffer);
        this.tableName = DataUtils.readString(byteBuffer, tableNameLen);
        this.blockPos = DataUtils.readLong(byteBuffer);
        this.rowId = DataUtils.readLong(byteBuffer);
        this.version = DataUtils.readInt(byteBuffer);
        this.type = byteBuffer.get();
        if (this.type != RowLogRecord.TYPE_INSERT) {
            this.oldRow = new RowData(byteBuffer);
        }
    }

    @Override
    public ByteBuffer getByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.totalByteLen);
        DataUtils.writeInt(byteBuffer, this.totalByteLen);
        DataUtils.writeLong(byteBuffer, this.startPos);
        DataUtils.writeInt(byteBuffer, this.transactionId);
        DataUtils.writeInt(byteBuffer, this.databaseId);

        DataUtils.writeInt(byteBuffer, this.tableName.length());
        DataUtils.writeStringData(byteBuffer, this.tableName, this.tableName.length());

        DataUtils.writeLong(byteBuffer, this.blockPos);
        DataUtils.writeLong(byteBuffer, this.rowId);
        DataUtils.writeInt(byteBuffer, this.version);
        byteBuffer.put(this.type);
        if (this.type != RowLogRecord.TYPE_INSERT) {
            byteBuffer.put(this.oldRow.getByteBuff());
        }
        // 获取真实长度
        this.totalByteLen = byteBuffer.position();
        byteBuffer.putInt(0, this.totalByteLen);
        byteBuffer.flip();
        return byteBuffer;
    }


    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getBlockPos() {
        return blockPos;
    }

    public void setBlockPos(long blockPos) {
        this.blockPos = blockPos;
    }

    public long getRowId() {
        return rowId;
    }

    public void setRowId(long rowId) {
        this.rowId = rowId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public RowData getOldRow() {
        return oldRow;
    }

    public void setOldRow(RowData oldRow) {
        this.oldRow = oldRow;
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
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

    public byte getType() {
        return type;
    }

}
