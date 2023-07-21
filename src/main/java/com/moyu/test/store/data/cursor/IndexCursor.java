package com.moyu.test.store.data.cursor;

import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.metadata.obj.Column;

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


    public IndexCursor(DataChunkStore dataChunkStore, Column[] columns, Column indexColumn, String indexPath) {
        this.dataChunkStore = dataChunkStore;
        this.columns = columns;
        this.indexColumn = indexColumn;
        this.indexPath = indexPath;
        this.nextPosIndex = 0;
        this.currChunkNextRowIndex = 0;

        if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
            BpTreeMap<Integer, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Integer.class);
            Integer key = Integer.valueOf(String.valueOf(indexColumn.getValue()));
            this.indexColumn.setValue(key);
            posArr = bpTreeMap.get(key);
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()){
            BpTreeMap<Long, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Long.class);
            Long key = Long.valueOf((String) indexColumn.getValue());
            this.indexColumn.setValue(key);
            posArr = bpTreeMap.get(Long.valueOf((String) indexColumn.getValue()));
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
            BpTreeMap<String, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, String.class);
            String key = (String)indexColumn.getValue();
            this.indexColumn.setValue(key);
            posArr = bpTreeMap.get(key);
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
