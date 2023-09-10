package com.moyu.test.net.model.terminal;

import com.moyu.test.net.model.BaseResultDto;
import com.moyu.test.net.util.ReadWriteUtil;
import com.moyu.test.session.Database;
import com.moyu.test.store.WriteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class DatabaseInfo implements BaseResultDto {

    private int totalByteLen;

    private Integer databaseId;

    private String name;

    private int tableNum;

    private List<String> tableNameList;

    public DatabaseInfo(Database database) {
        this(database.getDatabaseId(),database.getDbName(),new ArrayList<>(database.getTableMap().keySet()));
    }


    public DatabaseInfo(Integer databaseId, String name, List<String> tableNameList) {
        this.databaseId = databaseId;
        this.name = name;
        if (tableNameList != null) {
            this.tableNum = tableNameList.size();
        }
        this.tableNameList = tableNameList;
    }

    public DatabaseInfo(ByteBuffer byteBuffer) {
        this.totalByteLen = byteBuffer.getInt();
        this.databaseId = byteBuffer.getInt();
        this.name = ReadWriteUtil.readString(byteBuffer);
        this.tableNum = byteBuffer.getInt();
        this.tableNameList = new ArrayList<>();
        if (this.tableNum > 0) {
            for (int i = 0; i < tableNum; i++) {
                String tableName = ReadWriteUtil.readString(byteBuffer);
                this.tableNameList.add(tableName);
            }
        }
    }


    @Override
    public ByteBuffer getByteBuffer() {
        WriteBuffer writeBuffer = new WriteBuffer(128);
        writeBuffer.putInt(totalByteLen);
        writeBuffer.putInt(databaseId);
        ReadWriteUtil.writeString(writeBuffer, name);
        writeBuffer.putInt(tableNum);
        if (tableNum > 0) {
            for (String tableName : tableNameList) {
                ReadWriteUtil.writeString(writeBuffer, tableName);
            }
        }
        totalByteLen = writeBuffer.position();
        writeBuffer.putInt(0, totalByteLen);
        ByteBuffer buffer = writeBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }


    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTableNameList() {
        return tableNameList;
    }

    public void setTableNameList(List<String> tableNameList) {
        this.tableNameList = tableNameList;
    }


    @Override
    public String toString() {
        return "DatabaseInfo{" +
                "totalByteLen=" + totalByteLen +
                ", databaseId=" + databaseId +
                ", name='" + name + '\'' +
                ", tableNum=" + tableNum +
                ", tableNameList=" + tableNameList +
                '}';
    }
}
