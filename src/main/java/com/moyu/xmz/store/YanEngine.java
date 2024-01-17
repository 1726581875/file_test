package com.moyu.xmz.store;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.plan.SelectIndex;
import com.moyu.xmz.command.dml.sql.QueryTable;
import com.moyu.xmz.store.common.dto.OperateTableInfo;
import com.moyu.xmz.store.cursor.BTreeIndexCursor;
import com.moyu.xmz.store.cursor.BtreeCursor;
import com.moyu.xmz.store.cursor.Cursor;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.tree.BTreeStore;
import com.moyu.xmz.store.tree.Page;
import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.dbtype.AbstractColumnType;
import com.moyu.xmz.store.type.dbtype.LongColumnType;
import com.moyu.xmz.store.type.obj.ArrayDataType;
import com.moyu.xmz.store.type.obj.RowDataType;
import com.moyu.xmz.store.type.value.ArrayValue;
import com.moyu.xmz.store.type.value.LongValue;
import com.moyu.xmz.store.type.value.RowValue;
import com.moyu.xmz.store.type.value.Value;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.common.util.FileUtil;
import com.moyu.xmz.common.util.PathUtil;
import com.moyu.xmz.common.util.TypeConvertUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class YanEngine extends StoreEngine {


    public YanEngine(OperateTableInfo tableInfo) {
        super(tableInfo.getSession(), tableInfo.getTableName(), tableInfo.getTableColumns(), tableInfo.getCondition());
        super.allIndexList = tableInfo.getAllIndexList();
    }


    @Override
    public int insert(RowEntity rowEntity) {

        BTreeMap bTreeMap = null;
        try {
            bTreeMap = getBTreeMap();
            Column primaryKey = getPrimaryKey(rowEntity.getColumns());
            long nextRowId = bTreeMap.getNextRowId();
            rowEntity.setRowId(nextRowId);
            RowValue rowValue = new RowValue(0L, rowEntity.getColumns(), nextRowId);
            // 如果不存在主键，以行id作为b+树的key
            if (primaryKey == null) {
                bTreeMap.put(nextRowId, rowValue);
            } else {
                bTreeMap.put(primaryKey.getValue(), rowValue);
            }
            // 如果存在索引插入到对应索引树
            insertIndexTree(allIndexList, rowEntity, primaryKey);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            if(bTreeMap != null) {
                bTreeMap.close();
            }
        }
        return 1;
    }

    @Override
    public int batchFastInsert(List<RowEntity> rowList) {
        BTreeMap bTreeMap = null;
        try {
            bTreeMap = getBTreeMap();
            for (RowEntity rowEntity : rowList) {
                Column primaryKey = getPrimaryKey(rowEntity.getColumns());
                long nextRowId = bTreeMap.getNextRowId();
                RowValue rowValue = new RowValue(0L, rowEntity.getColumns(), nextRowId);
                // 如果不存在主键，以行id作为b+树的key
                if (primaryKey == null) {
                    bTreeMap.put(nextRowId, rowValue);
                } else {
                    bTreeMap.put(primaryKey.getValue(), rowValue);
                }

                // 如果存在索引插入到对应索引树
                insertIndexTree(allIndexList, rowEntity, primaryKey);

            }
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            bTreeMap.close();
        }
        return 1;
    }


    private void insertIndexTree(List<IndexMetadata> indexList, RowEntity row, Column primaryKey) {
        // 插入索引
        if (indexList != null && indexList.size() > 0) {
            for (IndexMetadata index : indexList) {
                Column indexColumnValue = getIndexColumnByColumnName(index.getColumnName(), row.getColumns());
                if (indexColumnValue != null && indexColumnValue.getValue() != null) {
                    insertIndexValue(index, row, primaryKey);
                }
            }
        }
    }



    private Column getPrimaryKey(Column[] columns) {
        for (Column column : columns) {
            if (column.getIsPrimaryKey() == (byte) 1) {
                return column;
            }
        }
        return null;
    }


    @Override
    public int update(Column[] updateColumns) {
        int updateNum = 0;
        BTreeMap bTreeMap = null;
        try {
            bTreeMap = getBTreeMap();
            Page page = bTreeMap.getFirstLeafPage();
            while (page != null) {
                List<RowValue> valueList = page.getValueList();
                int i = 0;
                while (i < valueList.size()) {
                    RowValue rowValue = valueList.get(i);
                    RowEntity rowEntity = rowValue.getRowEntity(tableColumns);
                    if (Expression.isMatch(rowEntity, condition)) {
                        // 更新数据
                        Column[] columns = rowEntity.getColumns();
                        for (Column updateColumn : updateColumns) {
                            columns[updateColumn.getColumnIndex()].setValue(updateColumn.getValue());
                        }
                        rowValue.setColumns(columns);
                        updateNum++;
                    }
                    i++;
                }

                page.commit();

                Long rightPos = page.getRightPos();
                if (rightPos == null || rightPos < 0) {
                    break;
                }
                page = bTreeMap.getPageByPos(rightPos);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return updateNum;
        } finally {
            if(bTreeMap != null) {
                bTreeMap.close();
            }
        }
        return updateNum;
    }

    @Override
    public int delete() {
        int deleteNum = 0;
        BTreeMap bTreeMap = null;
        try {
            bTreeMap = getBTreeMap();
            Page page = bTreeMap.getFirstLeafPage();
            while (page != null) {
                List<RowValue> valueList = page.getValueList();
                int i = 0;
                while (i < valueList.size()) {
                    RowValue rowValue = valueList.get(i);
                    RowEntity rowEntity = rowValue.getRowEntity(tableColumns);
                    if (Expression.isMatch(rowEntity, condition)) {
                        rowValue.setIsDeleted((byte) 1);
                        deleteNum++;

                        // 删除索引项
                        Column primaryKey = getPrimaryKey(rowEntity.getColumns());
                        if (allIndexList != null && allIndexList.size() > 0) {
                            for (IndexMetadata index : allIndexList) {
                                Column indexColumnValue = getIndexColumnByColumnName(index.getColumnName(), rowEntity.getColumns());
                                if (indexColumnValue != null && indexColumnValue.getValue() != null) {
                                    removeIndexItemValue(index, rowEntity, primaryKey);
                                }
                            }
                        }
                    }
                    i++;
                }
                page.commit();

                Long rightPos = page.getRightPos();
                if (rightPos == null || rightPos < 0) {
                    break;
                }
                page = bTreeMap.getPageByPos(rightPos);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return deleteNum;
        } finally {
            if(bTreeMap != null) {
                bTreeMap.close();
            }
        }
        return deleteNum;
    }

    @Override
    public void createIndex(Integer tableId, String indexName, String columnName, byte indexType) {
        IndexMetaFileAccessor indexMetaFileAccessor = null;
        BTreeStore bTreeIndexStore = null;
        try {
            indexMetaFileAccessor = new IndexMetaFileAccessor(session.getDatabase().getDatabaseId());
            IndexMetadata oldIndex = indexMetaFileAccessor.getIndex(tableId, indexName);
            // 存在则先删除索引元数据
            if (oldIndex != null) {
                indexMetaFileAccessor.dropIndexMetadata(tableId, indexName);
            }
            // 保存索引元数据
            IndexMetadata index = new IndexMetadata(0L, tableId, indexName, columnName, indexType);
            indexMetaFileAccessor.saveIndexMetadata(tableId, index);

            // 索引路径
            String indexPath = getIndexPath(indexName);
            // 索引文件存在则先删除
            File file = new File(indexPath);
            if (file.exists()) {
                file.delete();
            }
            // 创建索引文件
            FileUtil.createFileIfNotExists(indexPath);

            // 获取b-tree的键类型
            Column indexColumn = getIndexColumnByColumnName(columnName, tableColumns);
            bTreeIndexStore = new BTreeStore(indexPath);
            DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());

            BTreeMap bTreeIndexMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeIndexStore, false);

            // 遍历所有数据，构建b+树
            BTreeMap bTreeMap = getBTreeMap();
            Cursor cursor = new BtreeCursor(tableColumns, bTreeMap);
            RowEntity row = null;
            while ((row = cursor.next()) != null) {
                Column indexColumnValue = getIndexColumnByColumnName(columnName, row.getColumns());
                ArrayValue keyArrayValue = (ArrayValue) bTreeIndexMap.get(indexColumnValue.getValue());

                Column primaryKey = getPrimaryKey(row.getColumns());
                // 有主键则使用主键作为b-tree叶子节点的值，没有则使用行id作为值
                Value value = primaryKey != null ? getIndexValueObject(primaryKey) : new LongValue(row.getRowId());
                DataType valueArrItemType = primaryKey != null
                        ? AbstractColumnType.getDataType(primaryKey.getColumnType()) : new LongColumnType();
                // 把一级索引的键插入叶子节点作为二级索引的值
                keyArrayValue = insertNodeArray(value, valueArrItemType, keyArrayValue);
                bTreeIndexMap.putUnSaveDisk(indexColumnValue.getValue(), keyArrayValue);
            }
            bTreeIndexMap.commitSaveDisk();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DbException("创建索引发生异常");
        } finally {
            if(indexMetaFileAccessor != null) {
                indexMetaFileAccessor.close();
            }
            if(bTreeIndexStore != null) {
                bTreeIndexStore.close();
            }
        }
    }



    private void insertIndexValue(IndexMetadata index, RowEntity row, Column primaryKey) {
        BTreeStore bTreeIndexStore = null;
        try {
            // 索引路径
            String indexPath = getIndexPath(index.getIndexName());
            Column indexColumn = getIndexColumnByColumnName(index.getColumnName(), tableColumns);
            bTreeIndexStore = new BTreeStore(indexPath);
            DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());
            BTreeMap bTreeIndexMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeIndexStore, true);
            // 有主键则使用主键作为b-tree叶子节点的值，没有则使用行id作为值
            Value value = primaryKey != null ? getIndexValueObject(primaryKey) : new LongValue(row.getRowId());
            DataType valueArrItemType = primaryKey != null
                    ? AbstractColumnType.getDataType(primaryKey.getColumnType()) : new LongColumnType();

            Column indexColumnValue = getIndexColumnByColumnName(index.getColumnName(), row.getColumns());
            ArrayValue keyArrayValue = (ArrayValue) bTreeIndexMap.get(indexColumnValue.getValue());

            keyArrayValue = insertNodeArray(value, valueArrItemType, keyArrayValue);
            bTreeIndexMap.put(indexColumnValue.getValue(), keyArrayValue);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(bTreeIndexStore != null) {
                bTreeIndexStore.close();
            }
        }
    }


    private void removeIndexItemValue(IndexMetadata index, RowEntity row, Column primaryKey) {
        BTreeStore bTreeIndexStore = null;
        try {
            // 索引路径
            String indexPath = getIndexPath(index.getIndexName());
            Column indexColumn = getIndexColumnByColumnName(index.getColumnName(), tableColumns);
            bTreeIndexStore = new BTreeStore(indexPath);
            DataType keyDataType = AbstractColumnType.getDataType(indexColumn.getColumnType());
            BTreeMap bTreeIndexMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeIndexStore, true);
            // 有主键则使用主键作为b-tree叶子节点的值，没有则使用行id作为值
            Value value = primaryKey != null ? getIndexValueObject(primaryKey) : new LongValue(row.getRowId());
            DataType valueArrItemType = primaryKey != null
                    ? AbstractColumnType.getDataType(primaryKey.getColumnType()) : new LongColumnType();

            Column indexColumnValue = getIndexColumnByColumnName(index.getColumnName(), row.getColumns());
            ArrayValue keyArrayValue = (ArrayValue) bTreeIndexMap.get(indexColumnValue.getValue());

            if (keyArrayValue != null) {
                Value[] arr = keyArrayValue.getArr();
                List<Value> valueList = new ArrayList<>(arr.length);
                for (Value v : arr) {
                    if(v.compare(value) != 0) {
                        valueList.add(v);
                    }
                }
                keyArrayValue = new ArrayValue<>(valueList.toArray(new Value[0]), valueArrItemType);
            }
            bTreeIndexMap.put(indexColumnValue.getValue(), keyArrayValue);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(bTreeIndexStore != null) {
                bTreeIndexStore.close();
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
        BTreeMap clusteredIndexMap = getBTreeMap();
        if (table.getSelectIndex() == null) {
            System.out.println("不用索引，表:" + table.getTableName() + ",存储引擎:" + table.getEngineType());
            cursor = new BtreeCursor(table.getTableColumns(), clusteredIndexMap);
        } else {
            System.out.println("使用索引查询，索引:" + table.getSelectIndex().getIndexName()
                    + ",表:" + table.getTableName() + ",存储引擎:" + table.getEngineType());

            // todo 根据索引类型进行不同操作，如果是主键索引不需要进行回表操作

            ArrayValue keyArrValue = null;
            SelectIndex selectIndex = table.getSelectIndex();
            Column indexColumn = selectIndex.getIndexColumn();
            String indexPath = getIndexPath(selectIndex.getIndexName());
            BTreeStore bTreeIndexStore = null;
            try {
                bTreeIndexStore = new BTreeStore(indexPath);
                DataType keyDataType = AbstractColumnType.getDataType(selectIndex.getIndexColumn().getColumnType());
                BTreeMap bTreeIndexMap = new BTreeMap(keyDataType, new ArrayDataType(), bTreeIndexStore, true);
                Object value = TypeConvertUtil.convertValueType(String.valueOf(indexColumn.getValue()), indexColumn.getColumnType());
                keyArrValue = (ArrayValue) bTreeIndexMap.get(value);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (bTreeIndexStore != null) {
                    bTreeIndexStore.close();
                }
            }
            Object[] keyObjArray = null;
            Value[] keyArr = null;
            if (keyArrValue != null) {
                keyArr = keyArrValue.getArr();
                keyObjArray = new Object[keyArr.length];
                for (int i = 0; i < keyArr.length; i++) {
                    keyObjArray[i] = keyArr[i].getObjValue();
                }
            }
            cursor = new BTreeIndexCursor(table.getTableColumns(), clusteredIndexMap, keyObjArray);
        }
        return cursor;
    }


    private String getIndexPath(String indexName) {
        String dirPath = PathUtil.getBaseDirPath() + File.separator + this.session.getDatabaseId();
        String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
        return indexPath;
    }


    private BTreeMap getBTreeMap() throws IOException {
        BTreeStore bTreeStore = new BTreeStore(PathUtil.getYanEngineDataFilePath(session.getDatabaseId(), tableName));
        BTreeMap bTreeMap = null;
        try {
            Column primaryKey = getPrimaryKey(tableColumns);
            if (primaryKey == null) {
                bTreeMap = new BTreeMap(new LongColumnType(), new RowDataType(), bTreeStore, true);
            } else {
                DataType primaryType = AbstractColumnType.getDataType(primaryKey.getColumnType());
                bTreeMap = new BTreeMap(primaryType, new RowDataType(), bTreeStore, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            bTreeStore.close();
        }
        return bTreeMap;
    }


}
