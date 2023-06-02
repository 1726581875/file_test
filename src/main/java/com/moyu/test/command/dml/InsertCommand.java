package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.util.PathUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class InsertCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    private Column[] columns;

    private List<IndexMetadata> indexList;

    public InsertCommand(Integer databaseId, String tableName, Column[] columns, List<IndexMetadata> indexList) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.columns = columns;
        this.indexList = indexList;
    }

    @Override
    public String execute() {
        DataChunkStore dataChunkStore = null;
        try {
            // 插入数据
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            Long chunkPos = dataChunkStore.storeRowAndGetPos(columns);
            if(chunkPos == null) {
                return "插入数据失败";
            }
            // 插入索引
            if (indexList != null && indexList.size() > 0) {
                for (IndexMetadata index : indexList) {
                    Column indexColumn = getIndexColumn(index);
                    if (indexColumn != null && indexColumn.getValue() != null) {
                        insertIndex(index, indexColumn, chunkPos);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }


    private void insertIndex(IndexMetadata index, Column indexColumn, Long chunkPos) {
        String indexPath = PathUtil.getIndexFilePath(this.databaseId, this.tableName, index.getIndexName());
        if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
            insertIndexTree(indexPath, indexColumn, chunkPos, Integer.class);
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
            insertIndexTree(indexPath, indexColumn, chunkPos, Long.class);
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
            insertIndexTree(indexPath, indexColumn, chunkPos, String.class);
        } else {
            throw new SqlExecutionException("该字段类型不支持索引，类型:" + indexColumn.getColumnType());
        }
    }


    private <T extends Comparable> void insertIndexTree(String indexPath, Column indexColumn, Long startPos, Class<T> clazz) {
        BpTreeMap<T, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, clazz);
        T key = (T) indexColumn.getValue();
        Long[] arr = bpTreeMap.get(key);
        Long[] valueArr = BpTreeMap.insertValueArray(arr, startPos);
        bpTreeMap.put(key, valueArr);
    }


    private Column getIndexColumn(IndexMetadata index) {
        for (Column c : columns) {
            if (c.getColumnName().equals(index.getColumnName())) {
                return c;
            }
        }
        return null;
    }


    public String testWriteList(List<Column[]> columnsList) {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            List<byte[]> list = new ArrayList<>();

            for (int i = 0; i < columnsList.size(); i++) {
                Column[] columns = columnsList.get(i);
                byte[] rowBytes = RowData.toRowByteData(columns);
                list.add(rowBytes);
            }
            dataChunkStore.writeRow(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }


    public Column[] getColumns() {
        return columns;
    }
}
