package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.accessor.ColumnMetaFileAccessor;
import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.common.meta.ColumnMetadata;
import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.store.common.meta.TableMetadata;

import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class DescTableCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;


    public DescTableCommand(Integer databaseId, String tableName) {
        this.databaseId = databaseId;
        this.tableName = tableName;
    }


    @Override
    public QueryResult execCommand() {

        SelectColumn columnName = SelectColumn.newColumn("columnName", ColumnTypeConstant.CHAR);
        SelectColumn columnIndex = SelectColumn.newColumn("columnIndex", ColumnTypeConstant.INT_4);
        SelectColumn columnType = SelectColumn.newColumn("columnType", ColumnTypeConstant.CHAR);
        SelectColumn columnLength = SelectColumn.newColumn("columnLength", ColumnTypeConstant.INT_4);
        SelectColumn comment = SelectColumn.newColumn("comment", ColumnTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{columnName, columnIndex, columnType, columnLength, comment});

        TableMetaFileAccessor metadataStore = null;
        ColumnMetaFileAccessor columnMetaFileAccessor = null;
        IndexMetaFileAccessor indexStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(databaseId);
            columnMetaFileAccessor = new ColumnMetaFileAccessor(databaseId);
            List<TableMetadata> allData = metadataStore.getCurrDbAllTable();

            TableMetadata table = null;
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

            TableColumnBlock columnBlock = columnMetaFileAccessor.getColumnBlock(table.getTableId());
            if (columnBlock == null) {
                ExceptionUtil.throwSqlQueryException("表{}的字段块不存在", this.tableName);
            }
            // 构造输出结果
            List<ColumnMetadata> columnMetadataList = columnBlock.getColumnMetadataList();
            for (ColumnMetadata column : columnMetadataList) {
                queryResult.addRow(new Object[]{column.getColumnName(), column.getColumnIndex(),
                        ColumnTypeEnum.getNameByType(column.getColumnType()), column.getColumnLength()
                        , column.getComment()});
            }


            StringBuilder desc = new StringBuilder("");

            desc.append("存储引擎:" + table.getEngineType() + "\n");

            desc.append("index:\n");
            // 索引信息
            indexStore = new IndexMetaFileAccessor(databaseId);
            Map<Integer, TableIndexBlock> indexMap = indexStore.getIndexMap();
            TableIndexBlock tableIndexBlock = indexMap.get(table.getTableId());
            if (tableIndexBlock != null) {
                List<IndexMetadata> indexMetadataList = tableIndexBlock.getIndexMetadataList();
                if (indexMetadataList != null) {
                    for (IndexMetadata index : indexMetadataList) {
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
            if (columnMetaFileAccessor != null) {
                columnMetaFileAccessor.close();
            }
        }
        return queryResult;
    }
}
