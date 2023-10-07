package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.exception.SqlQueryException;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.*;

import java.util.ArrayList;
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
    public String[] exec() {
        List<String> resultList = new ArrayList<>();
        TableMetadataStore metadataStore = null;
        ColumnMetadataStore columnMetadataStore = null;
        IndexMetadataStore indexStore = null;
        try {
            metadataStore = new TableMetadataStore(databaseId);
            columnMetadataStore = new ColumnMetadataStore(databaseId);
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
                throw new SqlQueryException("表" + tableName + "不存在");
            }

            TableColumnBlock columnBlock = columnMetadataStore.getColumnBlock(table.getTableId());
            if (columnBlock == null) {
                throw new SqlQueryException("表字段不存在");
            }
            // 构造输出结果
            List<ColumnMetadata> columnMetadataList = columnBlock.getColumnMetadataList();
            resultList.add("columnName" + " | " + "columnIndex" + " | " + "columnType" + " | " + "columnLength");
            for (ColumnMetadata column : columnMetadataList) {
                resultList.add(column.getColumnName() + " | " + column.getColumnIndex() + " | " + ColumnTypeEnum.getNameByType(column.getColumnType()) + " | " + column.getColumnLength());
            }

            // 索引信息
            indexStore = new IndexMetadataStore(databaseId);
            Map<Integer, TableIndexBlock> indexMap = indexStore.getIndexMap();
            TableIndexBlock tableIndexBlock = indexMap.get(table.getTableId());
            if (tableIndexBlock != null) {
                List<IndexMetadata> indexMetadataList = tableIndexBlock.getIndexMetadataList();
                if (indexMetadataList != null) {
                    resultList.add("index:\n");
                    resultList.add("indexName" + " | " + "column" + " | " + "indexType");
                    for (IndexMetadata index : indexMetadataList) {
                        String type = index.getIndexType() == (byte) 1 ? "主键" : "一般索引";
                        resultList.add(index.getIndexName() + " | " + index.getColumnName() + " | " + type);
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
            if (columnMetadataStore != null) {
                columnMetadataStore.close();
            }
        }
        return resultList.toArray(new String[0]);
    }

    @Override
    public String execute() {
        String[] result = exec();
        StringBuilder stringBuilder = new StringBuilder();
        for (String str : result) {
            stringBuilder.append(str);
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }


    @Override
    public QueryResult execCommand() {

        SelectColumn columnName = SelectColumn.newColumn("columnName", ColumnTypeConstant.CHAR);
        SelectColumn columnIndex = SelectColumn.newColumn("columnIndex", ColumnTypeConstant.INT_4);
        SelectColumn columnType = SelectColumn.newColumn("columnType", ColumnTypeConstant.CHAR);
        SelectColumn columnLength = SelectColumn.newColumn("columnLength", ColumnTypeConstant.INT_4);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{columnName, columnIndex, columnType, columnLength});

        TableMetadataStore metadataStore = null;
        ColumnMetadataStore columnMetadataStore = null;
        IndexMetadataStore indexStore = null;
        try {
            metadataStore = new TableMetadataStore(databaseId);
            columnMetadataStore = new ColumnMetadataStore(databaseId);
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
                throw new SqlQueryException("表" + tableName + "不存在");
            }

            TableColumnBlock columnBlock = columnMetadataStore.getColumnBlock(table.getTableId());
            if (columnBlock == null) {
                throw new SqlQueryException("表字段不存在");
            }
            // 构造输出结果
            List<ColumnMetadata> columnMetadataList = columnBlock.getColumnMetadataList();
            for (ColumnMetadata column : columnMetadataList) {
                queryResult.addRow(new Object[]{column.getColumnName(), column.getColumnIndex(), ColumnTypeEnum.getNameByType(column.getColumnType()), column.getColumnLength()});
            }


            StringBuilder desc = new StringBuilder("");

            desc.append("存储引擎:" + table.getEngineType() + "\n");

            desc.append("index:\n");
            // 索引信息
            indexStore = new IndexMetadataStore(databaseId);
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
            if (columnMetadataStore != null) {
                columnMetadataStore.close();
            }
        }
        return queryResult;
    }
}
