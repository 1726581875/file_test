package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.SelectColumn;
import com.moyu.test.store.metadata.obj.TableMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class ShowTablesCommand extends AbstractCommand {

    private Integer databaseId;

    private List<TableMetadata> resultList = new ArrayList<>();


    public ShowTablesCommand(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public String[] getAllTable() {
        List<String> list = new ArrayList<>();
        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(databaseId);
            List<TableMetadata> allData = metadataStore.getCurrDbAllTable();

            for (int i = 0; i < allData.size(); i++) {
                list.add(allData.get(i).getTableName() + "  |  " + allData.get(i).getTableId());
            }
            resultList = allData;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        return list.toArray(new String[0]);
    }


    @Override
    public String execute() {
        String[] result = getAllTable();
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("----------------\n");
        stringBuilder.append("| 数据表 |   id  |\n");
        stringBuilder.append("----------------\n");
        for (String str : result) {
            stringBuilder.append("| " +str + " |");
            stringBuilder.append("\n");
        }
        stringBuilder.append("--------\n");
        stringBuilder.append("总数:"+ result.length +"\n");
        return stringBuilder.toString();
    }


    @Override
    public QueryResult execCommand() {

        long queryStartTime = System.currentTimeMillis();

        SelectColumn idColumn = SelectColumn.newColumn("id", ColumnTypeConstant.INT_4);
        SelectColumn nameColumn = SelectColumn.newColumn("tableName", ColumnTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{idColumn, nameColumn});

        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(databaseId);
            List<TableMetadata> allData = metadataStore.getCurrDbAllTable();

            for (int i = 0; i < allData.size(); i++) {
                TableMetadata table = allData.get(i);
                queryResult.addRow(new Object[]{table.getTableId(), table.getTableName()});
            }
            resultList = allData;
        } catch (Exception e) {
            e.printStackTrace();
            ExceptionUtil.throwSqlQueryException("查询数据库异常,databaseId={}", databaseId);
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        long queryEndTime = System.currentTimeMillis();
        String desc = "查询结果行数:" +  queryResult.getResultRows().size() + ", 耗时:" + (queryEndTime - queryStartTime)  + "ms";
        queryResult.setDesc(desc);
        return queryResult;
    }

    public List<TableMetadata> getResultList() {
        return resultList;
    }
}
