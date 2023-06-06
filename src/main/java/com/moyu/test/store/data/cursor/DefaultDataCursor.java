package com.moyu.test.store.data.cursor;

import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public class DefaultDataCursor implements Cursor {

    private Column[] columns;

    private DataChunkStore dataChunkStore;

    private DataChunk currChunk;

    private int nextChunkIndex;

    private int currChunkNextRowIndex;


    public DefaultDataCursor(DataChunkStore dataChunkStore, Column[] columns) {
        this.dataChunkStore = dataChunkStore;
        this.columns = columns;
        this.nextChunkIndex = 0;
        this.currChunkNextRowIndex = 0;
    }

    @Override
    public RowEntity next() {

        int dataChunkNum = dataChunkStore.getDataChunkNum();
        if (dataChunkNum == 0) {
            return null;
        }

        if(currChunk == null) {
            currChunk = dataChunkStore.getChunk(nextChunkIndex);
            nextChunkIndex++;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowData> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
            RowData rowData = dataRowList.get(currChunkNextRowIndex);
            Column[] columnData = rowData.getColumnData(columns);
            RowEntity dbRow = new RowEntity(columnData);
            currChunkNextRowIndex++;
            return dbRow;
        }

        // 遍历块，直到拿到数据
        int i = nextChunkIndex;
        while (true) {
            if(i > dataChunkNum - 1) {
                return null;
            }
            currChunk = dataChunkStore.getChunk(nextChunkIndex);
            currChunkNextRowIndex = 0;
            if(currChunk == null) {
                return null;
            }
            if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
                RowData rowData = dataRowList.get(currChunkNextRowIndex);
                Column[] columnData = rowData.getColumnData(columns);
                RowEntity dbRow = new RowEntity(columnData);
                currChunkNextRowIndex++;
                nextChunkIndex = i + 1;
                return dbRow;
            }
            i++;
        }
    }

}
