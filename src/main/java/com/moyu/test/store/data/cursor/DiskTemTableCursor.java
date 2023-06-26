package com.moyu.test.store.data.cursor;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.FileUtil;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/25
 * 磁盘临时表
 */
public class DiskTemTableCursor extends AbstractCursor {

    private Column[] columns;

    private String fullFilePath;

    private DataChunkStore dataChunkStore;

    private DataChunk currChunk;

    private int nextChunkIndex;

    private int currChunkNextRowIndex;


    public DiskTemTableCursor(String fullFilePath, Column[] columns) throws IOException {
        this.fullFilePath = fullFilePath;
        this.dataChunkStore = new DataChunkStore(this.fullFilePath);
        this.columns = columns;
        this.nextChunkIndex = DataChunkStore.FIRST_BLOCK_INDEX;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public RowEntity next() {

        if(closed) {
            throw new DbException("游标已关闭");
        }

        int dataChunkNum = dataChunkStore.getDataChunkNum();
        if (dataChunkNum == 0) {
            return null;
        }

        if(currChunk == null) {
            currChunk = dataChunkStore.getChunk(nextChunkIndex);
            nextChunkIndex++;
            currChunkNextRowIndex = 0;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowData> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
            RowData rowData = dataRowList.get(currChunkNextRowIndex);
            RowEntity dbRow = rowData.getRowEntity(columns);
            currChunkNextRowIndex++;
            return dbRow;
        }

        // 遍历块，直到拿到数据
        int i = nextChunkIndex;
        while (true) {
            if(i > dataChunkNum) {
                return null;
            }
            currChunk = dataChunkStore.getChunk(i);
            currChunkNextRowIndex = 0;
            if(currChunk == null) {
                return null;
            }
            dataRowList = currChunk.getDataRowList();
            if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
                RowData rowData = dataRowList.get(currChunkNextRowIndex);
                RowEntity dbRow = rowData.getRowEntity(columns);
                currChunkNextRowIndex++;
                nextChunkIndex = i + 1;
                return dbRow;
            }
            i++;
        }
    }


    @Override
    public void reset() {
        this.currChunk = null;
        this.nextChunkIndex = DataChunkStore.FIRST_BLOCK_INDEX;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }


    @Override
    void closeCursor() {
        dataChunkStore.close();
        FileUtil.deleteOnExists(fullFilePath);
    }
}
