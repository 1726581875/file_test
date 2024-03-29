package com.moyu.xmz.store;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.sql.QueryTable;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.store.cursor.Cursor;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.store.common.dto.TableInfo;
import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.value.*;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 * 存储引擎，定义存取操作数据的方式
 */
public abstract class StoreEngine {

    /**
     * 连接信息，包括用户会话信息、连接的数据库信息
     */
    protected ConnectSession session;
    /**
     * 操作的表名
     */
    protected String tableName;
    /**
     * 当前操作表的所有字段
     */
    protected Column[] tableColumns;
    /**
     * 当前操作表包含的所有索引信息
     */
    protected List<IndexMeta> allIndexList;


    public StoreEngine(ConnectSession session, String tableName, Column[] tableColumns) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
    }

    /**
     * 插入数据
     * @param rowEntity
     * @return
     */
    public abstract int insert(RowEntity rowEntity);

    /**
     * 批量插入数据
     * @param rowList
     * @return
     */
    public abstract int batchFastInsert(List<RowEntity> rowList);

    /**
     * 更新一行
     * @param updateColumns
     * @return
     */
    public abstract int update(Column[] updateColumns, Expression condition);

    /**
     * 删除，按照条件
     * @see this.condition
     * @return
     */
    public abstract int delete(Expression condition);

    /**
     * 创建索引
     * @param tableId
     * @param indexName
     * @param columnName
     * @param indexType
     */
    public abstract void createIndex(Integer tableId, String indexName, String columnName, byte indexType);

    /**
     * 获取数据遍历游标
     * 定义遍历所有数据方式
     * @param table
     * @return
     * @throws IOException
     */
    public abstract Cursor getQueryCursor(QueryTable table) throws IOException;


    /**
     * 字段变更
     */
    public boolean addOrDropColumnResetData(Column defColumn, Column[] newColumns, boolean isAdd) {
        return false;
    }


    public static StoreEngine getEngine(TableInfo tableInfo) {
        if(tableInfo.getEngineType() == null) {
            throw new IllegalArgumentException("引擎类型不能为空");
        }
        StoreEngine storeEngine = null;
        switch (tableInfo.getEngineType()) {
            case CommonConstant.ENGINE_TYPE_YU:
                storeEngine = new YuEngine(tableInfo);
                break;
            case CommonConstant.ENGINE_TYPE_YAN:
                storeEngine = new YanEngine(tableInfo);
                break;
            default:
                throw new IllegalArgumentException("不支持数据引擎:" + tableInfo.getEngineType());
        }
        return storeEngine;
    }



    protected Value getIndexValueObject(Column column) {
        switch (column.getColumnType()) {
            case DbTypeConstant.INT_4:
                return new IntegerValue((Integer) column.getValue());
            case DbTypeConstant.INT_8:
                return new LongValue((Long) column.getValue());
            case DbTypeConstant.VARCHAR:
            case DbTypeConstant.CHAR:
                return new StringValue(String.valueOf(column.getValue()));
            default:
                throw new DbException("不支持数据类型:" + column.getColumnType());
        }
    }

    protected ArrayValue insertNodeArray(Value value, DataType valueArrItemType, ArrayValue keyArrayValue) {
        if (keyArrayValue == null) {
            Value[] values = new Value[1];
            values[0] = value;
            keyArrayValue = new ArrayValue<>(values, valueArrItemType);
        } else {
            Value[] arr = keyArrayValue.getArr();
            Value[] values = BTreeMap.insertValueArray(arr, value);
            keyArrayValue = new ArrayValue<>(values, valueArrItemType);
        }
        return keyArrayValue;
    }


}
