package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.ColumnTypeEnum;
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
            columnMetadataStore = new ColumnMetadataStore();
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
            resultList.add("columnIndex" + " | " + "columnType" + " | " + "columnLength");
            for (ColumnMetadata column : columnMetadataList) {
                resultList.add(column.getColumnIndex() + " | " + ColumnTypeEnum.getNameByType(column.getColumnType()) + " | " + column.getColumnLength());
            }

            // 索引信息
            indexStore = new IndexMetadataStore();
            Map<Integer, TableIndexBlock> indexMap = indexStore.getIndexMap();
            TableIndexBlock tableIndexBlock = indexMap.get(table.getTableId());
            List<IndexMetadata> indexMetadataList = tableIndexBlock.getIndexMetadataList();
            if (indexMetadataList != null) {
                resultList.add("index:\n");
                for (IndexMetadata index : indexMetadataList) {
                    resultList.add("indexName:" + index.getIndexName() + ", column:" + index.getColumnName());
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

}
