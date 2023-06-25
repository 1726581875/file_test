package com.moyu.test.store.data.cursor;

import com.moyu.test.command.dml.sql.ConditionRange;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.data.tree.Page;
import com.moyu.test.store.metadata.obj.Column;

import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/6/22
 */
public class RangeIndexCursor extends AbstractCursor {

    private Column[] columns;

    private DataChunkStore dataChunkStore;

    private Column indexColumn;

    private String indexPath;

    /**
     * 当前索引页开始位置
     */
    private Long nextIndexPos;
    /**
     * 范围开始索引页位置
     */
    private Long startIndexPos;
    /**
     * 范围结束索引页位置
     */
    private Long endIndexPos;

    /**
     * 当前索引块地址
     */
    private Long[] posArr;

    private DataChunk currChunk;
    /**
     * 所以当前索引数组下一个位置
     */
    private int nextArrIndex;

    /**
     * 数据块内的行位置
     */
    private int nextChunkNextRowIndex;


    public RangeIndexCursor(DataChunkStore dataChunkStore, Column[] columns, ConditionRange range, String indexPath) {
        this.dataChunkStore = dataChunkStore;
        this.columns = columns;
        this.indexColumn = range.getColumn();
        this.indexPath = indexPath;
        this.nextArrIndex = 0;
        this.nextChunkNextRowIndex = 0;


        List<Long> pagePosList = new ArrayList<>();
        if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
            BpTreeMap<Integer, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Integer.class);
            String operator = range.getOperator();
            switch (operator) {
                case OperatorConstant.LESS_THAN:
                case OperatorConstant.LESS_THAN_OR_EQUAL:
                    // B+树左边第一个位置
                    startIndexPos = bpTreeMap.getFirstLeafPage().getStartPos();
                    nextIndexPos = startIndexPos;
                    // 范围查询最大上限
                    Integer upperLimit1 = Integer.valueOf(range.getUpperLimit());
                    endIndexPos = bpTreeMap.getPage(upperLimit1).getStartPos();
                    break;
                case OperatorConstant.GREATER_THAN:
                case OperatorConstant.GREATER_THAN_OR_EQUAL:
                    // B+树左边第一个位置
                    Integer lowerLimit1 = Integer.valueOf(range.getLowerLimit());
                    startIndexPos =  bpTreeMap.getPage(lowerLimit1).getStartPos();
                    nextIndexPos = startIndexPos;
                    break;
                case OperatorConstant.BETWEEN:
                    // 下限
                    Integer lowerLimit2 = Integer.valueOf(range.getLowerLimit());
                    startIndexPos =  bpTreeMap.getPage(lowerLimit2).getStartPos();
                    nextIndexPos = startIndexPos;
                    // 范围查询最大上限
                    Integer upperLimit2 = Integer.valueOf(range.getUpperLimit());
                    endIndexPos = bpTreeMap.getPage(upperLimit2).getStartPos();
                default:
                    throw new SqlIllegalException("sql语法有误");
            }
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
            BpTreeMap<Long, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Long.class);
            Long key = Long.valueOf((String) indexColumn.getValue());
            this.indexColumn.setValue(key);
            posArr = bpTreeMap.get(Long.valueOf((String) indexColumn.getValue()));
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
            BpTreeMap<String, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, String.class);
            String key = (String) indexColumn.getValue();
            this.indexColumn.setValue(key);
            posArr = bpTreeMap.get(key);
        }
    }



    @Override
    public RowEntity next() {

        if (closed) {
            throw new DbException("游标已关闭");
        }

        int dataChunkNum = dataChunkStore.getDataChunkNum();
        if (dataChunkNum == 0) {
            return null;
        }

        // 下一个索引块为空，数据块数组为空或者到结尾位置，并且数据块内为空或者当前数据块数据行到了结尾位置
        if (this.nextIndexPos == null
                && (this.posArr == null || this.nextArrIndex > this.posArr.length - 1)
                && (currChunk == null || !currDataBlackIsNotEnd())) {
            return null;
        }

        if (posArr == null || posArr.length == 0 || nextArrIndex > posArr.length - 1) {
            if (this.nextIndexPos != null
                    && (this.posArr == null || this.nextArrIndex > this.posArr.length - 1)
                    && (currChunk == null || !currDataBlackIsNotEnd())) {
                moveNextIndexBlack();
            }
        }

        if(currChunk == null) {
            Long pos = posArr[nextArrIndex];
            currChunk = dataChunkStore.getChunkByPos(pos);
            nextArrIndex++;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowData> dataRowList = currChunk.getDataRowList();
        if (currDataBlackIsNotEnd()) {
            while (nextChunkNextRowIndex < dataRowList.size()) {
                RowData rowData = dataRowList.get(nextChunkNextRowIndex);
                Column[] columnData = rowData.getColumnData(columns);
                RowEntity dbRow = new RowEntity(columnData);
                nextChunkNextRowIndex++;
                return dbRow;
            }
        }

        // 遍历块，直到拿到数据
        while (true) {

            // 下一个索引块为空，数据块数组为空或者到结尾位置，并且数据块内为空或者当前数据块数据行到了结尾位置
            if (this.nextIndexPos == null
                    && (this.posArr == null || this.nextArrIndex > this.posArr.length - 1)
                    && (currChunk == null || !currDataBlackIsNotEnd())) {
                return null;
            }

            if (posArr == null || posArr.length == 0 || nextArrIndex > posArr.length - 1) {
                if (this.nextIndexPos != null
                        && (this.posArr == null || this.nextArrIndex > this.posArr.length - 1)
                        && (currChunk == null || !currDataBlackIsNotEnd())) {
                    moveNextIndexBlack();
                }
            }

            int i = nextArrIndex;
            while (true) {
                if (i > posArr.length - 1) {
                    break;
                }
                Long pos = posArr[i];
                currChunk = dataChunkStore.getChunkByPos(pos);
                nextChunkNextRowIndex = 0;
                if (currChunk == null) {
                    break;
                }
                dataRowList = currChunk.getDataRowList();
                if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > nextChunkNextRowIndex) {
                    while (nextChunkNextRowIndex < dataRowList.size()) {
                        RowData rowData = dataRowList.get(nextChunkNextRowIndex);
                        Column[] columnData = rowData.getColumnData(columns);
                        RowEntity dbRow = new RowEntity(columnData);
                        nextChunkNextRowIndex++;
                        nextArrIndex = i + 1;
                        return dbRow;
                    }
                }
                i++;
            }
            nextArrIndex = i;

        }
    }

    private boolean currDataBlackIsNotEnd() {
        List<RowData> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > nextChunkNextRowIndex) {
            return true;
        } else {
            return false;
        }
    }


    private void moveNextIndexBlack() {
        BpTreeMap<Integer, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(this.indexPath, true, Integer.class);
        Page<Integer, Long[]> pageByPos = bpTreeMap.getPageByPos(this.nextIndexPos);
        List<Long[]> valueList = pageByPos.getValueList();
        Set<Long> posSet = new HashSet<>();
        for (Long[] pos : valueList) {
            posSet.addAll(Arrays.asList(pos));
        }
        this.posArr = posSet.toArray(new Long[0]);
        if (this.nextIndexPos.equals(this.endIndexPos)) {
            this.nextIndexPos = pageByPos.getRightPos();
        } else {
            this.nextIndexPos = null;
        }
        this.nextArrIndex = 0;
        this.nextChunkNextRowIndex = 0;
    }

    @Override
    public void reset() {
        currChunk = null;
        nextArrIndex = 0;
        nextChunkNextRowIndex = 0;
    }

    @Override
    public Column[] getColumns() {
        return columns;
    }


    @Override
    void closeCursor() {
        dataChunkStore.close();
    }


    public DataChunk getCurrChunk() {
        return currChunk;
    }
}
