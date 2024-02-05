package com.moyu.xmz.store.cursor;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.accessor.DataChunkAccessor;
import com.moyu.xmz.store.common.meta.RowMeta;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.common.util.FileUtils;

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

    private DataChunkAccessor dataChunkAccessor;

    private DataChunk currChunk;

    private int nextChunkIndex;

    private int currChunkNextRowIndex;


    public DiskTemTableCursor(String fullFilePath, Column[] columns) throws IOException {
        this.fullFilePath = fullFilePath;
        this.dataChunkAccessor = new DataChunkAccessor(this.fullFilePath);
        this.columns = columns;
        this.nextChunkIndex = DataChunkAccessor.FIRST_BLOCK_INDEX;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public RowEntity next() {

        if(closed) {
            throw new DbException("游标已关闭");
        }

        int dataChunkNum = dataChunkAccessor.getDataChunkNum();
        if (dataChunkNum == 0) {
            return null;
        }

        if(currChunk == null) {
            currChunk = dataChunkAccessor.getChunk(nextChunkIndex);
            nextChunkIndex++;
            currChunkNextRowIndex = 0;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowMeta> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
            RowMeta rowMeta = dataRowList.get(currChunkNextRowIndex);
            RowEntity dbRow = rowMeta.getRowEntity(columns);
            currChunkNextRowIndex++;
            return dbRow;
        }

        // 遍历块，直到拿到数据
        int i = nextChunkIndex;
        while (true) {
            if(i > dataChunkNum) {
                return null;
            }
            currChunk = dataChunkAccessor.getChunk(i);
            currChunkNextRowIndex = 0;
            if(currChunk == null) {
                return null;
            }
            dataRowList = currChunk.getDataRowList();
            if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
                RowMeta rowMeta = dataRowList.get(currChunkNextRowIndex);
                RowEntity dbRow = rowMeta.getRowEntity(columns);
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
        this.nextChunkIndex = DataChunkAccessor.FIRST_BLOCK_INDEX;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }


    @Override
    void closeCursor() {
        dataChunkAccessor.close();
        FileUtils.deleteOnExists(fullFilePath);
    }
}
