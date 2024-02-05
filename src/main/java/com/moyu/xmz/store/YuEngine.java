package com.moyu.xmz.store;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.sql.QueryTable;
import com.moyu.xmz.common.config.CommonConfig;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.common.util.FileUtil;
import com.moyu.xmz.common.util.PathUtil;
import com.moyu.xmz.store.accessor.DataChunkFileAccessor;
import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.OperateTableInfo;
import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.store.common.meta.RowMetadata;
import com.moyu.xmz.store.cursor.*;
import com.moyu.xmz.store.transaction.RowLogRecord;
import com.moyu.xmz.store.transaction.Transaction;
import com.moyu.xmz.store.transaction.TransactionManager;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.tree.BTreeStore;
import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.AbstractColumnType;
import com.moyu.xmz.store.type.dbtype.LongType;
import com.moyu.xmz.store.type.obj.ArrayDataType;
import com.moyu.xmz.store.type.value.ArrayValue;
import com.moyu.xmz.store.type.value.LongValue;
import com.moyu.xmz.store.type.value.Value;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class YuEngine extends StoreEngine {

    public YuEngine(OperateTableInfo tableInfo) {
        super(tableInfo.getSession(), tableInfo.getTableName(), tableInfo.getTableColumns(), tableInfo.getCondition());
        super.allIndexList = tableInfo.getAllIndexList();
    }

    @Override
    public int insert(RowEntity rowEntity) {
        DataChunkFileAccessor dataChunkFileAccessor = null;
        try {
            // 插入数据
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), this.tableName);
            dataChunkFileAccessor = new DataChunkFileAccessor(fileFullPath);

            long rowId = dataChunkFileAccessor.getNextRowId();
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
            Long chunkPos = dataChunkFileAccessor.storeRowAndGetPos(rowEntity.getColumns(), rowId);
            if(chunkPos == null) {
                throw new SqlExecutionException("插入数据失败");
            }

            // 插入到磁盘后才知道块位置，重新记录事务信息
            if(transaction != null) {
                transaction.setStartPos(chunkPos);
                TransactionManager.recordTransaction(transaction);
            }

            // 插入索引
            if (allIndexList != null && allIndexList.size() > 0) {
                for (IndexMetadata index : allIndexList) {
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
            dataChunkFileAccessor.close();
        }

        return 1;
    }

    @Override
    public int batchFastInsert(List<RowEntity> rowList) {
        int num = 0;
        DataChunkFileAccessor dataChunkFileAccessor = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), tableName);
            dataChunkFileAccessor = new DataChunkFileAccessor(fileFullPath);
            List<byte[]> list = new ArrayList<>();
            for (RowEntity row : rowList) {
                list.add(row.getColumnDataBytes());
            }
            dataChunkFileAccessor.writeRow(list);
            num = list.size();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkFileAccessor.close();
        }

        return num;
    }

    private void insertIndex(IndexMetadata index, Column indexColumn, Long chunkPos) {
        // 索引路径
        String dirPath = PathUtil.getBaseDirPath() + File.separator + this.session.getDatabaseId();
        String indexPath = dirPath + File.separator + tableName + "_" + index.getIndexName() + ".idx";
        DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());
        BTreeStore bTreeStore = null;
        try {
            bTreeStore = new BTreeStore(indexPath);
            BTreeMap<Comparable, ArrayValue> bpTreeMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeStore, true);
            ArrayValue array = bpTreeMap.get((Comparable) indexColumn.getValue());
            ArrayValue arrayValue = insertNodeArray(new LongValue(chunkPos), new LongType(), array);
            bpTreeMap.put((Comparable) indexColumn.getValue(), arrayValue);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(bTreeStore != null) {
                bTreeStore.close();
            }
        }
    }

    @Override
    public int update(Column[] updateColumns) {
        int updateRowNum = 0;
        DataChunkFileAccessor dataChunkFileAccessor = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), this.tableName);
            dataChunkFileAccessor = new DataChunkFileAccessor(fileFullPath);
            int dataChunkNum = dataChunkFileAccessor.getDataChunkNum();
            // 遍历数据块
            for (int i = 1; i <= dataChunkNum; i++) {
                DataChunk chunk = dataChunkFileAccessor.getChunk(i);
                if (chunk == null) {
                    break;
                }
                List<RowMetadata> dataRowList = chunk.getDataRowList();

                for (int j = 0; j < dataRowList.size(); j++) {
                    RowMetadata rowMetadata = dataRowList.get(j);
                    Column[] columnData = rowMetadata.getColumnData(tableColumns);

                    boolean match = Expression.isMatch(new RowEntity(columnData), condition);
                    if (match) {

                        // 如果存在事务，记录旧值到到undo log
                        Transaction transaction = TransactionManager.getTransaction(session.getTransactionId());
                        if(transaction != null) {
                            RowLogRecord record = new RowLogRecord(this.tableName, rowMetadata, RowLogRecord.TYPE_UPDATE);
                            record.setBlockPos(chunk.getStartPos());
                            record.setDatabaseId(session.getDatabaseId());
                            record.setRowId(rowMetadata.getRowId());
                            record.setTransactionId(transaction.getTransactionId());
                            transaction.addRowLogRecord(record);
                            TransactionManager.recordTransaction(transaction);
                        }

                        // 更新数据
                        for (Column newValueColumn : updateColumns) {
                            columnData[newValueColumn.getColumnIndex()].setValue(newValueColumn.getValue());
                        }

                        RowMetadata newRow = new RowMetadata(rowMetadata.getStartPos(), RowMetadata.toRowByteData(columnData), rowMetadata.getRowId());
                        chunk.updateRow(j, newRow);
                        updateRowNum++;

                        // TODO 更新索引
                    }
                }
                // 更新块整个数据块到磁盘
                dataChunkFileAccessor.updateChunk(chunk);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new SqlExecutionException("执行更新语句发生异常");
        } finally {
            dataChunkFileAccessor.close();
        }
        return updateRowNum;
    }


    @Override
    public int delete() {
        int deleteRowNum = 0;
        DataChunkFileAccessor dataChunkFileAccessor = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(session.getDatabaseId(), this.tableName);
            dataChunkFileAccessor = new DataChunkFileAccessor(fileFullPath);
            // TODO 应该要支持按索引删除
            deleteRowNum = deleteDataByCondition(dataChunkFileAccessor);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkFileAccessor.close();
        }
        return deleteRowNum;
    }

    @Override
    public void createIndex(Integer tableId, String indexName, String columnName, byte indexType) {
        DataChunkFileAccessor dataChunkFileAccessor = null;
        IndexMetaFileAccessor indexStore = null;
        try {
            indexStore = new IndexMetaFileAccessor(session.getDatabase().getDatabaseId());
            IndexMetadata oldIndex = indexStore.getIndex(tableId, indexName);
            // 存在则先删除索引元数据
            if (oldIndex != null) {
                indexStore.dropIndexMetadata(tableId, indexName);
            }

            // 保存索引元数据
            IndexMetadata index = new IndexMetadata(0L, tableId, indexName, columnName, indexType);
            indexStore.saveIndexMetadata(tableId, index);

            // 索引路径
            String dirPath = PathUtil.getBaseDirPath() + File.separator + this.session.getDatabaseId();
            String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
            // 索引文件存在则先删除
            File file = new File(indexPath);
            if (file.exists()) {
                file.delete();
            }
            // 创建索引文件
            FileUtil.createFileIfNotExists(indexPath);

            // 操作数据文件，获取数据块数量(后面遍历没一行数据，为每一行数据创建索引)
            dataChunkFileAccessor = new DataChunkFileAccessor(PathUtil.getDataFilePath(this.session.getDatabaseId(), this.tableName));
            int dataChunkNum = dataChunkFileAccessor.getDataChunkNum();
            // 构造索引树
            buildIndexTree(indexPath, dataChunkNum,columnName, dataChunkFileAccessor);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkFileAccessor.close();
            indexStore.close();
        }
    }


    private <T extends Comparable> void buildIndexTree(String indexPath, Integer dataChunkNum, String columnName, DataChunkFileAccessor dataChunkFileAccessor) {
        Column indexColumn = getIndexColumnByColumnName(columnName, tableColumns);
        DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());
        BTreeStore bTreeStore = null;
        try {
            bTreeStore = new BTreeStore(indexPath);
            // 创建一颗b+树
            BTreeMap<T, ArrayValue> bpTreeMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeStore,false);
            // 一行一行遍历数据
            for (int i = 0; i < dataChunkNum; i++) {
                DataChunk chunk = dataChunkFileAccessor.getChunk(i);
                for (int j = 0; j < chunk.getDataRowList().size(); j++) {
                    RowMetadata rowMetadata = chunk.getDataRowList().get(j);
                    Column[] columnData = rowMetadata.getColumnData(tableColumns);
                    // 找到对应索引字段
                    Column indexColumnValue = getIndexColumnByColumnName(columnName, columnData);
                    if (indexColumnValue != null) {
                        ArrayValue array = bpTreeMap.get((T) indexColumnValue.getValue());
                        ArrayValue arrayValue = insertNodeArray(new LongValue(chunk.getStartPos()),  new LongType(), array);
                        bpTreeMap.putUnSaveDisk((T) indexColumnValue.getValue(), arrayValue);
                    }
                }
            }
            // 保存到磁盘
            bpTreeMap.commitSaveDisk();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(bTreeStore != null) {
                bTreeStore.close();
            }
        }
    }


    private Column getIndexColumnByColumnName(String columnName, Column[] columns) {
        for (Column c : columns) {
            if (columnName.equals(c.getColumnName())) {
                return c;
            }
        }
        return null;
    }


    @Override
    public Cursor getQueryCursor(QueryTable table) throws IOException {
        Cursor cursor = null;
        DataChunkFileAccessor dataChunkFileAccessor = new DataChunkFileAccessor(PathUtil.getDataFilePath(this.session.getDatabaseId(), table.getTableName()));
        if (table.getSelectIndex() == null) {
            System.out.println("不用索引，表:" + table.getTableName() + ",存储引擎:" + table.getEngineType());
            cursor = new DefaultCursor(dataChunkFileAccessor, table.getTableColumns());
            // 如果是小表，直接读取整个表的数据到内存
            if(dataChunkFileAccessor.getDataChunkNum() * DataChunk.DATA_CHUNK_LEN <= CommonConfig.TABLE_IN_MEMORY_MAX_SIZE) {
                cursor =  convertToInMemoryCursor(cursor);
            }

        } else if(table.getSelectIndex() != null && table.getSelectIndex().isRangeQuery()){
            System.out.println("使用索引查询(范围)，索引:" + table.getSelectIndex().getIndexName()
                    + ",表:" + table.getTableName() + ",存储引擎:" + table.getEngineType());
            String indexPath = PathUtil.getIndexFilePath(this.session.getDatabaseId(), table.getTableName(), table.getSelectIndex().getIndexName());
            cursor = new RangeIndexCursor(dataChunkFileAccessor, table.getTableColumns(), table.getSelectIndex().getCondition() , indexPath);
        } else {
            System.out.println("使用索引查询，索引:" + table.getSelectIndex().getIndexName()
                    + ",表:" + table.getTableName() + ",存储引擎:" + table.getEngineType());
            String indexPath = PathUtil.getIndexFilePath(this.session.getDatabaseId(), table.getTableName(), table.getSelectIndex().getIndexName());
            cursor = new IndexCursor(dataChunkFileAccessor, table.getTableColumns(), table.getSelectIndex().getIndexColumn(), indexPath);
        }
        return cursor;
    }

    private Cursor convertToInMemoryCursor(Cursor diskCursor) {
        List<RowEntity> rows = new LinkedList();
        RowEntity row;
        while ((row = diskCursor.next()) != null) {
            if (!row.isDeleted()) {
                rows.add(row);
            }
        }
        return new MemoryTemTableCursor(new ArrayList<>(rows), diskCursor.getColumns());
    }

    /**
     * 不使用索引
     * @param dataChunkFileAccessor
     * @return
     */
    private int deleteDataByCondition(DataChunkFileAccessor dataChunkFileAccessor) {
        int dataChunkNum = dataChunkFileAccessor.getDataChunkNum();
        int deleteRowNum = 0;
        // 遍历数据块
        for (int i = 1; i <= dataChunkNum; i++) {
            DataChunk chunk = dataChunkFileAccessor.getChunk(i);
            if (chunk == null) {
                break;
            }
            List<RowMetadata> dataRowList = chunk.getDataRowList();
            int k = dataRowList.size() - 1;
            do {
                RowMetadata rowMetadata = dataRowList.get(k);
                Column[] columnData = rowMetadata.getColumnData(tableColumns);
                boolean compareResult = Expression.isMatch(new RowEntity(columnData), condition);
                // 只移除符合条件的行
                if (compareResult) {

                    // 如果存在事务，记录旧值到到undo log
                    Transaction transaction = TransactionManager.getTransaction(session.getTransactionId());
                    if(transaction != null) {
                        RowLogRecord record = new RowLogRecord(this.tableName, rowMetadata, RowLogRecord.TYPE_DELETE);
                        record.setBlockPos(chunk.getStartPos());
                        record.setDatabaseId(session.getDatabaseId());
                        record.setRowId(rowMetadata.getRowId());
                        record.setTransactionId(transaction.getTransactionId());
                        transaction.addRowLogRecord(record);
                        TransactionManager.recordTransaction(transaction);
                    }

                    // 删除行
                    chunk.markRowIsDeleted(k);
                    // 删除主键索引
                    if (allIndexList != null && allIndexList.size() > 0) {
                        for (IndexMetadata index : allIndexList) {
                            if (index.getIndexType() == CommonConstant.PRIMARY_KEY) {
                                Column indexColumn = getIndexColumn(index);
                                removePrimaryKeyValue(index, indexColumn, chunk.getStartPos());
                            }
                        }
                    }
                    deleteRowNum++;
                }
                k--;
            } while (k >= 0);

            // 更新块整个数据块到磁盘
            dataChunkFileAccessor.updateChunk(chunk);
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


    private void removePrimaryKeyValue(IndexMetadata index, Column indexColumn, Long chunkPos) {

        if(indexColumn == null || indexColumn.getValue() == null) {
            return;
        }
        // 索引路径
        String dirPath = PathUtil.getBaseDirPath() + File.separator + this.session.getDatabaseId();
        String indexPath = dirPath + File.separator + tableName + "_" + index.getIndexName() + ".idx";
        DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());
        BTreeStore bTreeStore = null;
        try {
            bTreeStore = new BTreeStore(indexPath);
            BTreeMap<Comparable, ArrayValue> bpTreeMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeStore, true);
            ArrayValue array = bpTreeMap.get((Comparable) indexColumn.getValue());
            if(array == null || array.getArr() == null) {
                return;
            } else {
                Value[] arr = array.getArr();
                List<Value> valueList = new ArrayList<>(arr.length);
                LongValue longValue = new LongValue(chunkPos);
                for (Value v : arr) {
                    if(v.compare(longValue) != 0) {
                        valueList.add(v);
                    }
                }
                ArrayValue<Value> arrayValue = new ArrayValue<>(valueList.toArray(new Value[0]), new LongType());
                bpTreeMap.put((Comparable) indexColumn.getValue(), arrayValue);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(bTreeStore != null) {
                bTreeStore.close();
            }
        }
    }

}
