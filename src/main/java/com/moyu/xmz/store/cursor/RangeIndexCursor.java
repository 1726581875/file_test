package com.moyu.xmz.store.cursor;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.expression.SingleComparison;
import com.moyu.xmz.common.constant.OperatorConstant;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.common.exception.SqlIllegalException;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.accessor.DataChunkFileAccessor;
import com.moyu.xmz.store.common.meta.RowMetadata;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.tree.Page;
import com.moyu.xmz.store.type.value.ArrayValue;
import com.moyu.xmz.store.type.value.LongValue;
import com.moyu.xmz.store.type.value.Value;
import com.moyu.xmz.store.common.dto.Column;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author xiaomingzhang
 * @date 2023/6/22
 */
public class RangeIndexCursor extends AbstractCursor {

    private Column[] columns;

    private DataChunkFileAccessor dataChunkFileAccessor;

    private String indexPath;

    /**
     * 索引块地址
     */
    private Long[] posArr;

    private DataChunk currChunk;

    private int nextPosIndex;

    private int currChunkNextRowIndex;


    public RangeIndexCursor(DataChunkFileAccessor dataChunkFileAccessor, Column[] columns, Expression range, String indexPath) {
        this.dataChunkFileAccessor = dataChunkFileAccessor;
        this.columns = columns;
        this.indexPath = indexPath;
    }



    private <T extends Comparable> void init(BTreeMap<T, ArrayValue> bpTreeMap
            , byte columnType , SingleComparison less, SingleComparison greater){

        Long startIndexPos = null;
        Long endIndexPos = null;

        String operator = "";
        switch (operator) {
            case OperatorConstant.LESS_THAN:
            case OperatorConstant.LESS_THAN_OR_EQUAL:
                // B+树左边第一个位置
                startIndexPos = bpTreeMap.getFirstLeafPage().getStartPos();
                // 范围查询最大上限
/*                T upperLimit1 = (T) TypeConvertUtil.convertValueType(range.getUpperLimit(), columnType);
                endIndexPos = bpTreeMap.getPage(upperLimit1).getStartPos();*/
                break;
            case OperatorConstant.GREATER_THAN:
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
/*                T lowerLimit1 = (T) TypeConvertUtil.convertValueType(range.getLowerLimit(), columnType);
                startIndexPos =  bpTreeMap.getPage(lowerLimit1).getStartPos();*/
                break;
            case OperatorConstant.BETWEEN:
                // 下限
/*                T lowerLimit2 = (T) TypeConvertUtil.convertValueType(range.getLowerLimit(), columnType);
                startIndexPos =  bpTreeMap.getPage(lowerLimit2).getStartPos();*/
                // 范围查询最大上限
/*                T upperLimit2 = (T) TypeConvertUtil.convertValueType(range.getUpperLimit(), columnType);
                endIndexPos = bpTreeMap.getPage(upperLimit2).getStartPos();*/
                break;
            default:
                throw new SqlIllegalException("sql语法有误");
        }

        initAllDataChunkPos(bpTreeMap, startIndexPos, endIndexPos);
    }


    private <T extends Comparable> void  initAllDataChunkPos(BTreeMap<T, ArrayValue> bpTreeMap, Long startIndexPos, Long endIndexPos) {
        Set<Long> posSet = new HashSet<>();
        Long nextIndexPos = startIndexPos;
        while (nextIndexPos != null) {
            Page<T, ArrayValue> pageByPos = bpTreeMap.getPageByPos(nextIndexPos);
            List<ArrayValue> arrayValueList = pageByPos.getValueList();
            for (ArrayValue arr : arrayValueList) {
                if(arr != null && arr.getArr() != null) {
                    for (Value v : arr.getArr()) {
                        LongValue longValue = (LongValue) v;
                        posSet.add(longValue.getValue());
                    }
                }
            }
            // 到了结束位置
            if(nextIndexPos.equals(endIndexPos)) {
                nextIndexPos = null;
            } else {
                nextIndexPos = pageByPos.getRightPos();
            }
        }
        this.posArr = posSet.toArray(new Long[0]);
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

        if(posArr == null || posArr.length == 0) {
            return null;
        }

        if(nextPosIndex > posArr.length - 1 &&
                (currChunk == null  || currChunk.getDataRowList().size() <= currChunkNextRowIndex)) {
            return null;
        }

        if(currChunk == null) {
            Long pos = posArr[nextPosIndex];
            currChunk = dataChunkFileAccessor.getChunkByPos(pos);
            nextPosIndex++;
        }

        if(currChunk == null) {
            return null;
        }

        // 从当前块拿
        List<RowMetadata> dataRowList = currChunk.getDataRowList();
        if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
            while (currChunkNextRowIndex < dataRowList.size()) {
                RowMetadata rowMetadata = dataRowList.get(currChunkNextRowIndex);
                Column[] columnData = rowMetadata.getColumnData(columns);
                RowEntity dbRow = new RowEntity(columnData);
                currChunkNextRowIndex++;
                return dbRow;
            }
        }

        // 遍历块，直到拿到数据
        int i = nextPosIndex;
        while (true) {
            if(i > posArr.length - 1) {
                return null;
            }
            Long pos = posArr[i];
            currChunk = dataChunkFileAccessor.getChunkByPos(pos);
            currChunkNextRowIndex = 0;
            if(currChunk == null) {
                return null;
            }
            dataRowList = currChunk.getDataRowList();
            if (dataRowList != null && dataRowList.size() > 0 && dataRowList.size() > currChunkNextRowIndex) {
                while (currChunkNextRowIndex < dataRowList.size()) {
                    RowMetadata rowMetadata = dataRowList.get(currChunkNextRowIndex);
                    Column[] columnData = rowMetadata.getColumnData(columns);
                    RowEntity dbRow = new RowEntity(columnData);
                    currChunkNextRowIndex++;
                    nextPosIndex = i + 1;
                    return dbRow;
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
        dataChunkFileAccessor.close();
    }


    public DataChunk getCurrChunk() {
        return currChunk;
    }
}
