package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.dml.sql.condition.ConditionComparator;
import com.moyu.test.command.dml.sql.ConditionTree2;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.session.ConnectSession;
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
 * @date 2023/5/18
 */
public class DeleteCommand extends AbstractCommand {

    private String tableName;

    private Column[] columns;

    private ConditionTree2 conditionTree;

    private List<IndexMetadata> indexList;

    private ConnectSession session;


    public DeleteCommand(ConnectSession session,
                         String tableName,
                         Column[] columns,
                         ConditionTree2 conditionTree) {
        this.session = session;
        this.tableName = tableName;
        this.columns = columns;
        this.conditionTree = conditionTree;
    }

    @Override
    public String execute() {
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

        return "共删除了" + deleteRowNum + "行数据";
    }

    private int deleteAllData(DataChunkStore dataChunkStore) {
        int dataChunkNum = dataChunkStore.getDataChunkNum();
        int deleteRowNum = 0;
        // 遍历数据块
        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            deleteRowNum += chunk.getRowNum();
            // 清空块内所有行数据
            chunk.clear();
            // 更新块整个数据块到磁盘
            dataChunkStore.updateChunk(chunk);

        }

        // 清空所有索引
        if (indexList != null && indexList.size() > 0) {
            for (IndexMetadata index : indexList) {
                //Column indexColumn = getIndexColumn(index, columns);
                String indexPath = PathUtil.getIndexFilePath(session.getDatabaseId(), this.tableName, index.getIndexName());
                BpTreeMap<Comparable, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Comparable.class);
                bpTreeMap.clear();
            }
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
                Column[] columnData = rowData.getColumnData(columns);
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
                                Column indexColumn = getIndexColumn(index, columnData);
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

    private Column getIndexColumn(IndexMetadata index,  Column[] columnData) {
        for (Column c : columns) {
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



    public List<IndexMetadata> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<IndexMetadata> indexList) {
        this.indexList = indexList;
    }
}
