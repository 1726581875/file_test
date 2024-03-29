package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.StoreEngine;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.TableInfo;
import com.moyu.xmz.store.common.meta.ColumnMeta;

/**
 * @author xiaomingzhang
 * @date 2023/5/31
 */
public class AlterTableCmd extends AbstractCmd {

    private static final String OPE_ADD = "ADD";

    private static final String OPE_DROP = "DROP";

    private TableInfo tableInfo;
    /**
     * 操作方式
     */
    private String operation;
    /**
     * 字段
     */
    private Column column;

    public AlterTableCmd(TableInfo tableInfo, String operation, Column column) {
        this.tableInfo = tableInfo;
        this.operation = operation;
        this.column = column;
    }

    @Override
    public QueryResult exec() {
        boolean isSuccess = false;
        ColumnMetaAccessor columnAcc = null;
        try {
            columnAcc = new ColumnMetaAccessor(tableInfo.getSession().getDatabaseId());
            TableColumnBlock columnBlock = columnAcc.getColumnBlock(tableInfo.getTable().getTableId());
            if (columnBlock == null) {
                throw ExceptionUtil.buildDbException("操作失败，字段block为空", tableInfo.getTable().getTableName());
            }

            StoreEngine engineOperation = StoreEngine.getEngine(tableInfo);
            Table table = tableInfo.getTable();
            switch (this.operation) {
                case OPE_ADD:
                    // 更新元数据信息
                    ColumnMeta columnMeta = new ColumnMeta(table.getTableId(), -1, column);
                    columnBlock.addColumn(columnMeta);
                    columnAcc.saveColumnBlock(columnBlock);
                    // 更新数据
                    boolean r = engineOperation.addOrDropColumnResetData(this.column, table.getColumns(true), true);
                    break;
                case OPE_DROP:
                    columnBlock.removeColumn(column.getColumnName());
                    columnAcc.saveColumnBlock(columnBlock);
                    columnAcc.saveColumnBlock(columnBlock);
                    // 更新数据
                    boolean r1 = engineOperation.addOrDropColumnResetData(this.column, table.getColumns(true), false);
                    break;
                default:
                    throw ExceptionUtil.buildDbException("alter table操作方式{}不支持", this.operation);
            }
            isSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (columnAcc != null) {
                columnAcc.close();
            }
        }

        return isSuccess ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }
}
