package com.moyu.test.store.operation;

import com.moyu.test.command.dml.sql.ConditionComparator;
import com.moyu.test.command.dml.sql.FromTable;
import com.moyu.test.store.data.cursor.BtreeCursor;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.BTreeStore;
import com.moyu.test.store.data2.Page;
import com.moyu.test.store.data2.type.RowValue;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.type.AbstractColumnType;
import com.moyu.test.store.type.DataType;
import com.moyu.test.store.type.LongColumnType;
import com.moyu.test.store.type.RowDataType;
import com.moyu.test.util.PathUtil;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public class YanEngineOperation extends BasicOperation {


    public YanEngineOperation(OperateTableInfo tableInfo) {
        super(tableInfo.getSession(), tableInfo.getTableName(), tableInfo.getTableColumns(), tableInfo.getConditionTree());
    }


    @Override
    public int insert(RowEntity rowEntity) {

        BTreeMap bTreeMap = null;
        try {
            bTreeMap = getBTreeMap();
            Column primaryKey = getPrimaryKey(rowEntity.getColumns());
            long nextRowId = bTreeMap.getNextRowId();
            RowValue rowValue = new RowValue(0L, rowEntity.getColumns(), nextRowId);
            // 如果不存在主键，以行id作为b+树的key
            if(primaryKey == null) {
                bTreeMap.put(nextRowId, rowValue);
            } else {
                bTreeMap.put(primaryKey.getValue(), rowValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            bTreeMap.close();
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
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            bTreeMap.close();
        }
        return 1;
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
                    if (ConditionComparator.isMatch(rowEntity, conditionTree)) {
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
            bTreeMap.close();
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
                    if (ConditionComparator.isMatch(rowEntity, conditionTree)) {
                        rowValue.setIsDeleted((byte)0);
                        deleteNum++;
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
            bTreeMap.close();
        }
        return deleteNum;
    }

    @Override
    public Cursor getQueryCursor(FromTable table) throws IOException {
        System.out.println("不用索引，table:" + table.getTableName() + ",存储引擎:" + table.getEngineType());
        BTreeMap bTreeMap = getBTreeMap();
        Cursor cursor = null;
        cursor = new BtreeCursor(table.getTableColumns(), bTreeMap);
        return cursor;
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
