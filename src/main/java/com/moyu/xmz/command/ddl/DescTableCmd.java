package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;
import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.common.meta.ColumnMeta;
import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.store.common.meta.TableMeta;

import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class DescTableCmd extends AbstractCmd {

    private Integer databaseId;

    private String tableName;


    public DescTableCmd(Integer databaseId, String tableName) {
        this.databaseId = databaseId;
        this.tableName = tableName;
    }


    @Override
    public QueryResult execCommand() {

        SelectColumn columnName = SelectColumn.newColumn("columnName", DbTypeConstant.CHAR);
        SelectColumn columnIndex = SelectColumn.newColumn("columnIndex", DbTypeConstant.INT_4);
        SelectColumn columnType = SelectColumn.newColumn("columnType", DbTypeConstant.CHAR);
        SelectColumn columnLength = SelectColumn.newColumn("columnLength", DbTypeConstant.INT_4);
        SelectColumn comment = SelectColumn.newColumn("comment", DbTypeConstant.CHAR);
        SelectColumn isNotNull = SelectColumn.newColumn("isNotNull", DbTypeConstant.INT_4);
        SelectColumn defaultVal = SelectColumn.newColumn("defaultVal", DbTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{columnName, columnIndex, columnType, columnLength, comment, isNotNull, defaultVal});

        TableMetaAccessor metadataStore = null;
        ColumnMetaAccessor columnMetaAccessor = null;
        IndexMetaAccessor indexStore = null;
        try {
            metadataStore = new TableMetaAccessor(databaseId);
            columnMetaAccessor = new ColumnMetaAccessor(databaseId);
            List<TableMeta> allData = metadataStore.getCurrDbAllTable();

            TableMeta table = null;
            for (int i = 0; i < allData.size(); i++) {
                if (databaseId.equals(allData.get(i).getDatabaseId())
                        && tableName.equals(allData.get(i).getTableName())) {
                    table = allData.get(i);
                    break;
                }
            }

            if (table == null) {
                ExceptionUtil.throwSqlQueryException("表{}不存在", this.tableName);
            }

            TableColumnBlock columnBlock = columnMetaAccessor.getColumnBlock(table.getTableId());
            if (columnBlock == null) {
                ExceptionUtil.throwSqlQueryException("表{}的字段块不存在", this.tableName);
            }
            // 构造输出结果
            List<ColumnMeta> columnMetaList = columnBlock.getColumnMetaList();
            for (ColumnMeta column : columnMetaList) {
                queryResult.addRow(new Object[]{column.getColumnName(), column.getColumnIndex(),
                        ColumnTypeEnum.getNameByType(column.getColumnType()), column.getColumnLength()
                        , column.getComment(), column.getIsNotNull(), column.getDefaultVal()});
            }


            StringBuilder desc = new StringBuilder("");

            desc.append("存储引擎:" + table.getEngineType() + "\n");

            desc.append("index:\n");
            // 索引信息
            indexStore = new IndexMetaAccessor(databaseId);
            Map<Integer, TableIndexBlock> indexMap = indexStore.getIndexMap();
            TableIndexBlock tableIndexBlock = indexMap.get(table.getTableId());
            if (tableIndexBlock != null) {
                List<IndexMeta> indexMetaList = tableIndexBlock.getIndexMetaList();
                if (indexMetaList != null) {
                    for (IndexMeta index : indexMetaList) {
                        String type = index.getIndexType() == (byte) 1 ? "主键" : "普通索引";
                        desc.append(index.getIndexName() + "(" + index.getColumnName() + ") " +  type + " btree\n");
                    }
                }
            }
            queryResult.setDesc(desc.toString());

            return queryResult;

        } catch (Exception e) {
            e.printStackTrace();
            ExceptionUtil.throwSqlQueryException("查询数据库发生异常");
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
            if (columnMetaAccessor != null) {
                columnMetaAccessor.close();
            }
        }
        return queryResult;
    }
}
