package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.common.meta.TableMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class ShowTablesCmd extends AbstractCmd {

    private Integer databaseId;

    private List<TableMeta> resultList = new ArrayList<>();


    public ShowTablesCmd(Integer databaseId) {
        this.databaseId = databaseId;
    }

    @Override
    public QueryResult execCommand() {

        long queryStartTime = System.currentTimeMillis();

        SelectColumn idColumn = SelectColumn.newColumn("id", DbTypeConstant.INT_4);
        SelectColumn nameColumn = SelectColumn.newColumn("tableName", DbTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{idColumn, nameColumn});

        TableMetaAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaAccessor(databaseId);
            List<TableMeta> allData = metadataStore.getCurrDbAllTable();

            for (int i = 0; i < allData.size(); i++) {
                TableMeta table = allData.get(i);
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

    public List<TableMeta> getResultList() {
        return resultList;
    }
}
