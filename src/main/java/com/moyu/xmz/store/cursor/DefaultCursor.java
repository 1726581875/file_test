package com.moyu.xmz.store.cursor;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.accessor.DataChunkFileAccessor;
import com.moyu.xmz.store.common.meta.RowMetadata;
import com.moyu.xmz.store.common.dto.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public class DefaultCursor extends AbstractCursor {

    private Column[] columns;

    private DataChunkFileAccessor dataChunkFileAccessor;

    private DataChunk currChunk;

    private int nextChunkIndex;

    private int currChunkNextRowIndex;


    public DefaultCursor(DataChunkFileAccessor dataChunkFileAccessor, Column[] columns) {
        this.dataChunkFileAccessor = dataChunkFileAccessor;
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
    }

}
