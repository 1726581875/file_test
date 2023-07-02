package com.moyu.test.store.operation;

import com.moyu.test.command.dml.sql.ConditionComparator;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.store.transaction.RowLogRecord;
import com.moyu.test.store.transaction.Transaction;
import com.moyu.test.store.transaction.TransactionManager;
import com.moyu.test.util.PathUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class YuEngineOperation extends BasicOperation {

    private List<IndexMetadata> indexList;


    public YuEngineOperation(OperateTableInfo tableInfo) {
        super(tableInfo.getSession(), tableInfo.getTableName(), tableInfo.getTableColumns(), tableInfo.getConditionTree());
    }

    public void setIndexList(List<IndexMetadata> indexList) {
        this.indexList = indexList;
    }

    @Override
    public int insert(RowEntity rowEntity) {
        DataChunkStore dataChunkStore = null;
        try {
            // 插入数据
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);

            long rowId = dataChunkStore.getNextRowId();
            // 如果存在事务，记录旧值到到undo log
            Transaction transaction = TransactionManager.getTransaction(session.getTransactionId());
            if(transaction != null) {
                RowLogRecord record = new RowLogRecord(this.tableName, null, RowLogRecord.TYPE_INSERT);
                // TODO 一开始并不知道位置
                record.setBlockPos(-1L);
                record.setDatabaseId(session.getDatabaseId());
                record.setRowId(rowId);
                record.setTransactionId(transaction.getTransactionId());
                transaction.addRowLogRecord(record);
                TransactionManager.recordTransaction(transaction);
            }

            // 存储数据
            Long chunkPos = dataChunkStore.storeRowAndGetPos(rowEntity.getColumns(), rowId);
            if(chunkPos == null) {
                throw new SqlExecutionException("插入数据失败");
            }

            // 插入到磁盘后才知道块位置，重新记录事务信息
            if(transaction != null) {
                transaction.setStartPos(chunkPos);
                TransactionManager.recordTransaction(transaction);
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
            return 0;
        } finally {
            dataChunkStore.close();
        }

        return 1;
    }

    private void insertIndex(IndexMetadata index, Column indexColumn, Long chunkPos) {
        String indexPath = PathUtil.getIndexFilePath(session.getDatabaseId(), this.tableName, index.getIndexName());
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

    @Override
    public int update(Column[] updateColumns) {
        int updateRowNum = 0;
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
            // 遍历数据块
            for (int i = 1; i <= dataChunkNum; i++) {
                DataChunk chunk = dataChunkStore.getChunk(i);
                if (chunk == null) {
                    break;
                }
                List<RowData> dataRowList = chunk.getDataRowList();

                for (int j = 0; j < dataRowList.size(); j++) {
                    RowData rowData = dataRowList.get(j);
                    Column[] columnData = rowData.getColumnData(tableColumns);

                    boolean match = ConditionComparator.isMatch(new RowEntity(columnData), conditionTree);
                    if (match) {

                        // 如果存在事务，记录旧值到到undo log
                        Transaction transaction = TransactionManager.getTransaction(session.getTransactionId());
                        if(transaction != null) {
                            RowLogRecord record = new RowLogRecord(this.tableName, rowData, RowLogRecord.TYPE_UPDATE);
                            record.setBlockPos(chunk.getStartPos());
                            record.setDatabaseId(session.getDatabaseId());
                            record.setRowId(rowData.getRowId());
                            record.setTransactionId(transaction.getTransactionId());
                            transaction.addRowLogRecord(record);
                            TransactionManager.recordTransaction(transaction);
                        }

                        // 更新数据
                        for (Column newValueColumn : updateColumns) {
                            columnData[newValueColumn.getColumnIndex()].setValue(newValueColumn.getValue());
                        }

                        RowData newRow = new RowData(rowData.getStartPos(), RowData.toRowByteData(columnData), rowData.getRowId());
                        chunk.updateRow(j, newRow);
                        updateRowNum++;
                    }
                }
                // 更新块整个数据块到磁盘
                dataChunkStore.updateChunk(chunk);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new SqlExecutionException("执行更新语句发生异常");
        } finally {
            dataChunkStore.close();
        }
        return updateRowNum;
    }


    @Override
    public int delete() {
        int deleteRowNum = 0;
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            // TODO 应该要支持按索引删除
            deleteRowNum = deleteDataByCondition(dataChunkStore);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return deleteRowNum;
    }

    /**
     * 不使用索引
     * @param dataChunkStore
     * @return
     */
    private int deleteDataByCondition(DataChunkStore dataChunkStore) {
        int dataChunkNum = dataChunkStore.getDataChunkNum();
        int deleteRowNum = 0;
        // 遍历数据块
        for (int i = 1; i <= dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            List<RowData> dataRowList = chunk.getDataRowList();
            int k = dataRowList.size() - 1;
            do {
                RowData rowData = dataRowList.get(k);
                Column[] columnData = rowData.getColumnData(tableColumns);
                boolean compareResult = ConditionComparator.isMatch(new RowEntity(columnData), conditionTree);
                // 只移除符合条件的行
                if (compareResult) {

                    // 如果存在事务，记录旧值到到undo log
                    Transaction transaction = TransactionManager.getTransaction(session.getTransactionId());
                    if(transaction != null) {
                        RowLogRecord record = new RowLogRecord(this.tableName, rowData, RowLogRecord.TYPE_DELETE);
                        record.setBlockPos(chunk.getStartPos());
                        record.setDatabaseId(session.getDatabaseId());
                        record.setRowId(rowData.getRowId());
                        record.setTransactionId(transaction.getTransactionId());
                        transaction.addRowLogRecord(record);
                        TransactionManager.recordTransaction(transaction);
                    }

                    // 删除行
                    chunk.markRowIsDeleted(k);
                    // 删除主键索引
                    if (indexList != null && indexList.size() > 0) {
                        for (IndexMetadata index : indexList) {
                            if (index.getIndexType() == CommonConstant.PRIMARY_KEY) {
                                Column indexColumn = getIndexColumn(index);
                                removePrimaryKeyValue(index, indexColumn);
                            }
                        }
                    }
                    deleteRowNum++;
                }
                k--;
            } while (k >= 0);

            // 更新块整个数据块到磁盘
            dataChunkStore.updateChunk(chunk);
        }

        return deleteRowNum;
    }

    private Column getIndexColumn(IndexMetadata index) {
        for (Column c : tableColumns) {
            if (c.getColumnName().equals(index.getColumnName())) {
                return c;
            }
        }
        return null;
    }


    private void removePrimaryKeyValue(IndexMetadata index, Column indexColumn) {

        if(indexColumn == null || indexColumn.getValue() == null) {
            return;
        }

        String indexPath = PathUtil.getIndexFilePath(session.getDatabaseId(), this.tableName, index.getIndexName());
        if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
            BpTreeMap<Integer, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Integer.class);
            Integer key = (Integer) indexColumn.getValue();
            bpTreeMap.remove(key);
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
            BpTreeMap<Long, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Long.class);
            Long key = (Long) indexColumn.getValue();
            bpTreeMap.remove(key);
        } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
            BpTreeMap<String, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, String.class);
            String key = (String) indexColumn.getValue();
            bpTreeMap.remove(key);
        } else {
            throw new SqlExecutionException("该字段类型不支持索引，类型:" + indexColumn.getColumnType());
        }
    }

}
