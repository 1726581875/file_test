package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.common.meta.TableMetadata;

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
        TableMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(databaseId);
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
    public QueryResult execCommand() {

        long queryStartTime = System.currentTimeMillis();

        SelectColumn idColumn = SelectColumn.newColumn("id", ColumnTypeConstant.INT_4);
        SelectColumn nameColumn = SelectColumn.newColumn("tableName", ColumnTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{idColumn, nameColumn});

        TableMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(databaseId);
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
