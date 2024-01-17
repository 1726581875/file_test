package com.moyu.xmz.store;

import com.moyu.xmz.command.dml.expression.Expression;
import com.moyu.xmz.command.dml.sql.QueryTable;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.store.cursor.Cursor;
import com.moyu.xmz.store.cursor.RowEntity;
import com.moyu.xmz.store.tree.BTreeMap;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.store.common.dto.OperateTableInfo;
import com.moyu.xmz.store.type.DataType;
import com.moyu.xmz.store.type.value.*;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public abstract class StoreEngine {

    protected ConnectSession session;

    protected String tableName;

    protected Column[] tableColumns;

    protected Expression condition;

    protected List<IndexMetadata> allIndexList;


    public StoreEngine(ConnectSession session, String tableName, Column[] tableColumns, Expression condition) {
        this.session = session;
        this.tableName = tableName;
        this.tableColumns = tableColumns;
        this.condition = condition;
    }

    public abstract int insert(RowEntity rowEntity);

    public abstract int batchFastInsert(List<RowEntity> rowList);

    public abstract int update(Column[] updateColumns);

    public abstract int delete();

    public abstract void createIndex(Integer tableId, String indexName, String columnName, byte indexType);


    public abstract Cursor getQueryCursor(QueryTable table) throws IOException;


    public static StoreEngine getEngineOperation(OperateTableInfo tableInfo) {
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
            case ColumnTypeConstant.INT_4:
                return new IntegerValue((Integer) column.getValue());
            case ColumnTypeConstant.INT_8:
                return new LongValue((Long) column.getValue());
            case ColumnTypeConstant.VARCHAR:
            case ColumnTypeConstant.CHAR:
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
