package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.exception.ExceptionUtil;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;
import com.moyu.test.store.metadata.obj.SelectColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class ShowDatabasesCommand extends AbstractCommand {

    private List<DatabaseMetadata> resultList;


    public String[] execAndGetResult() {
        List<String> list = new ArrayList<>();
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore();
            List<DatabaseMetadata> allData = metadataStore.getAllData();
            resultList = allData;
            for (int i = 0; i < allData.size(); i++) {
                list.add(allData.get(i).getName() + "  |  " + allData.get(i).getDatabaseId());
            }
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
        String[] result = execAndGetResult();
        StringBuilder stringBuilder = new StringBuilder();


        stringBuilder.append("--------------\n");
        stringBuilder.append("| 数据库 |  id |\n");
        stringBuilder.append("---------------\n");
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
        SelectColumn idColumn = SelectColumn.newColumn("id", ColumnTypeConstant.INT_4);
        SelectColumn nameColumn = SelectColumn.newColumn("dbName", ColumnTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{idColumn, nameColumn});
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore();
            List<DatabaseMetadata> allData = metadataStore.getAllData();
            resultList = allData;
            for (int i = 0; i < allData.size(); i++) {
                DatabaseMetadata metadata = allData.get(i);
                queryResult.addRow(new Object[]{metadata.getDatabaseId(), metadata.getName()});
            }
        } catch (Exception e) {
            e.printStackTrace();
            ExceptionUtil.throwSqlQueryException("查询数据库异常");
        } finally {
            if (metadataStore != null) {
                metadataStore.close();
            }
        }
        return queryResult;
    }

    public List<DatabaseMetadata> getResultList() {
        return resultList;
    }
}
