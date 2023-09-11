package com.moyu.test.store.operation;

import com.moyu.test.command.dml.expression.Expression;
import com.moyu.test.command.dml.sql.QueryTable;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.DbException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.data.cursor.Cursor;
import com.moyu.test.store.data.cursor.RowEntity;
import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.type.*;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.store.type.DataType;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/1
 */
public abstract class BasicOperation {

    protected ConnectSession session;

    protected String tableName;

    protected Column[] tableColumns;

    protected Expression condition;

    protected List<IndexMetadata> allIndexList;


    public BasicOperation(ConnectSession session, String tableName, Column[] tableColumns, Expression condition) {
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


    public static BasicOperation getEngineOperation(OperateTableInfo tableInfo) {
        if(tableInfo.getEngineType() == null) {
            throw new IllegalArgumentException("引擎类型不能为空");
        }
        BasicOperation basicOperation = null;
        switch (tableInfo.getEngineType()) {
            case CommonConstant.ENGINE_TYPE_YU:
                basicOperation = new YuEngineOperation(tableInfo);
                break;
            case CommonConstant.ENGINE_TYPE_YAN:
                basicOperation = new YanEngineOperation(tableInfo);
                break;
            default:
                throw new IllegalArgumentException("不支持数据引擎:" + tableInfo.getEngineType());
        }
        return basicOperation;
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
