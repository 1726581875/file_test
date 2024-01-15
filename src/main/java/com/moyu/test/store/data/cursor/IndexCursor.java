package com.moyu.test.store.data.cursor;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.BTreeStore;
import com.moyu.test.store.data2.type.ArrayValue;
import com.moyu.test.store.data2.type.LongValue;
import com.moyu.test.store.data2.type.Value;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.dbtype.AbstractColumnType;
import com.moyu.test.store.type.obj.ArrayDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/6
 */
public class IndexCursor extends AbstractCursor {

    private Column[] columns;

    private DataChunkStore dataChunkStore;

    private Column indexColumn;

    private String indexPath;

    /**
     * 索引块地址
     */
    private Long[] posArr;

    private DataChunk currChunk;

    private int nextPosIndex;

    private int currChunkNextRowIndex;


    public IndexCursor(DataChunkStore dataChunkStore, Column[] columns, Column indexColumn, String indexPath) throws IOException {
        this.dataChunkStore = dataChunkStore;
        this.columns = columns;
        this.indexColumn = indexColumn;
        this.indexPath = indexPath;
        this.nextPosIndex = 0;
        this.currChunkNextRowIndex = 0;

        DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());
        BTreeStore bTreeStore = new BTreeStore(indexPath);
        BTreeMap<Comparable, ArrayValue> bpTreeMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeStore, true);
        ArrayValue array = bpTreeMap.get((Comparable) indexColumn.getValue());

        if(array != null && array.getArr() != null) {
            List<Long> posList = new ArrayList<>();
            for (Value v : array.getArr()) {
                posList.add(((LongValue)v).getValue());
            }
            posArr = posList.toArray(new Long[0]);
        }



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

        if(posArr == null || posArr.length == 0) {
            return null;
        }

        if(nextPosIndex > posArr.length - 1 &&
                (currChunk == null  || currChunk.getDataRowList().size() <= currChunkNextRowIndex)) {
            return null;
        }

        if(currChunk == null) {
            Long pos = posArr[nextPosIndex];
            currChunk = dataChunkStore.getChunkByPos(pos);
            nextPosIndex++;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowData> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
            while (currChunkNextRowIndex < dataRowList.size()) {
                RowData rowData = dataRowList.get(currChunkNextRowIndex);
                Column[] columnData = rowData.getColumnData(columns);
                RowEntity dbRow = new RowEntity(columnData);
                currChunkNextRowIndex++;
                if (isIndexRow(dbRow)) {
                    return dbRow;
                }
            }
        }

        // 遍历块，直到拿到数据
        int i = nextPosIndex;
        while (true) {
            if(i > posArr.length - 1) {
                return null;
            }
            Long pos = posArr[i];
            currChunk = dataChunkStore.getChunkByPos(pos);
            currChunkNextRowIndex = 0;
            if(currChunk == null) {
                return null;
            }
            dataRowList = currChunk.getDataRowList();
            if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
                while (currChunkNextRowIndex < dataRowList.size()) {
                    RowData rowData = dataRowList.get(currChunkNextRowIndex);
                    Column[] columnData = rowData.getColumnData(columns);
                    RowEntity dbRow = new RowEntity(columnData);
                    currChunkNextRowIndex++;
                    if (isIndexRow(dbRow)) {
                        nextPosIndex = i + 1;
                        return dbRow;
                    }
                }
            }
            i++;
        }
    }

    @Override
    public void reset() {
        currChunk = null;
        nextPosIndex = 0;
        currChunkNextRowIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }


    @Override
    void closeCursor() {
        dataChunkStore.close();
    }


    private boolean isIndexRow(RowEntity dbRow) {
        for (Column c : dbRow.getColumns()) {
            if(c.getColumnName().equals(indexColumn.getColumnName())
                    && c.getValue().equals(indexColumn.getValue())) {
                return true;
            }
        }
        return false;
    }
}
