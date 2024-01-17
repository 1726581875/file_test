package com.moyu.xmz.store.cursor;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.accessor.DataChunkFileAccessor;
import com.moyu.xmz.store.common.meta.RowMetadata;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.common.util.FileUtil;

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

    private DataChunkFileAccessor dataChunkFileAccessor;

    private DataChunk currChunk;

    private int nextChunkIndex;

    private int currChunkNextRowIndex;


    public DiskTemTableCursor(String fullFilePath, Column[] columns) throws IOException {
        this.fullFilePath = fullFilePath;
        this.dataChunkFileAccessor = new DataChunkFileAccessor(this.fullFilePath);
        this.columns = columns;
        this.nextChunkIndex = DataChunkFileAccessor.FIRST_BLOCK_INDEX;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public RowEntity next() {

        if(closed) {
            throw new DbException("游标已关闭");
        }

        int dataChunkNum = dataChunkFileAccessor.getDataChunkNum();
        if (dataChunkNum == 0) {
            return null;
        }

        if(currChunk == null) {
            currChunk = dataChunkFileAccessor.getChunk(nextChunkIndex);
            nextChunkIndex++;
            currChunkNextRowIndex = 0;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowMetadata> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
            RowMetadata rowMetadata = dataRowList.get(currChunkNextRowIndex);
            RowEntity dbRow = rowMetadata.getRowEntity(columns);
            currChunkNextRowIndex++;
            return dbRow;
        }

        // 遍历块，直到拿到数据
        int i = nextChunkIndex;
        while (true) {
            if(i > dataChunkNum) {
                return null;
            }
            currChunk = dataChunkFileAccessor.getChunk(i);
            currChunkNextRowIndex = 0;
            if(currChunk == null) {
                return null;
            }
            dataRowList = currChunk.getDataRowList();
            if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
                RowMetadata rowMetadata = dataRowList.get(currChunkNextRowIndex);
                RowEntity dbRow = rowMetadata.getRowEntity(columns);
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
        this.nextChunkIndex = DataChunkFileAccessor.FIRST_BLOCK_INDEX;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }


    @Override
    void closeCursor() {
        dataChunkFileAccessor.close();
        FileUtil.deleteOnExists(fullFilePath);
    }
}
