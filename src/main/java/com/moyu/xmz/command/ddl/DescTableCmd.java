package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.util.CollectionUtils;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.common.meta.IndexMeta;

import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class DescTableCmd extends AbstractCmd {

    private Database database;

    private String tableName;

    public DescTableCmd(Database database, String tableName) {
        this.database = database;
        this.tableName = tableName;
    }

    @Override
    public QueryResult execCommand() {
        QueryResult queryResult = new QueryResult();
        SelectColumn[] selectColumns = {
                SelectColumn.newColumn("columnName", DbTypeConstant.CHAR),
                SelectColumn.newColumn("columnIndex", DbTypeConstant.INT_4),
                SelectColumn.newColumn("columnType", DbTypeConstant.CHAR),
                SelectColumn.newColumn("columnLength", DbTypeConstant.INT_4),
                SelectColumn.newColumn("comment", DbTypeConstant.CHAR),
                SelectColumn.newColumn("isNotNull", DbTypeConstant.INT_4),
                SelectColumn.newColumn("defaultVal", DbTypeConstant.CHAR)
        };
        queryResult.setSelectColumns(selectColumns);

        IndexMetaAccessor indexMetaAccessor = null;
        try {
            Table table = this.database.getTable(this.tableName);
            if (table == null) {
                ExceptionUtil.throwSqlQueryException("表{}不存在", this.tableName);
            }
            // 表的字段信息
            for (Column column : table.getColumns()) {
                queryResult.addRow(new Object[]{
                        column.getColumnName(),
                        column.getColumnIndex(),
                        ColumnTypeEnum.getNameByType(column.getColumnType()),
                        column.getColumnLength(),
                        column.getComment(),
                        Integer.valueOf(column.getIsNotNull()),
                        column.getDefaultVal()
                });
            }
            // 拼接存储引擎、索引等信息
            StringBuilder desc = new StringBuilder("");
            desc.append("存储引擎:" + table.getEngineType() + "\n");
            desc.append("索引:");
            indexMetaAccessor = new IndexMetaAccessor(this.database.getDatabaseId());
            Map<Integer, TableIndexBlock> indexMap = indexMetaAccessor.getIndexMap();
            TableIndexBlock indexBlock = indexMap.get(table.getTableId());
            if (indexBlock != null && CollectionUtils.isNotEmpty(indexBlock.getIndexMetaList())) {
                desc.append("\n");
                for (IndexMeta index : indexBlock.getIndexMetaList()) {
                    String type = index.getIndexType() == (byte) 1 ? "主键" : "普通索引";
                    desc.append(index.getIndexName() + "(" + index.getColumnName() + ") " + type + " btree\n");
                }
            } else {
                desc.append("无\n");
            }
            queryResult.setDesc(desc.toString());
            return queryResult;

        } catch (Exception e) {
            e.printStackTrace();
            ExceptionUtil.throwSqlQueryException("查询发生异常");
        } finally {
            if (indexMetaAccessor != null) {
                indexMetaAccessor.close();
            }
        }
        return queryResult;
    }
}
