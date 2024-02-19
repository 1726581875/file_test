package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.accessor.DatabaseMetaAccessor;
import com.moyu.xmz.store.common.meta.DatabaseMeta;
import com.moyu.xmz.store.common.dto.SelectColumn;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class ShowDatabasesCmd extends AbstractCmd {

    private List<DatabaseMeta> resultList;

    @Override
    public QueryResult execCommand() {

        long queryStartTime = System.currentTimeMillis();

        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{
                SelectColumn.newColumn("id", DbTypeConstant.INT_4),
                SelectColumn.newColumn("dbName", DbTypeConstant.CHAR)
        });
        DatabaseMetaAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaAccessor();
            List<DatabaseMeta> allData = metadataStore.getAllData();
            resultList = allData;
            for (int i = 0; i < allData.size(); i++) {
                DatabaseMeta metadata = allData.get(i);
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

        long queryEndTime = System.currentTimeMillis();
        String desc = "查询结果行数:" + queryResult.getResultRows().size() + ", 耗时:" + (queryEndTime - queryStartTime) + "ms";
        queryResult.setDesc(desc);

        return queryResult;
    }

    public List<DatabaseMeta> getResultList() {
        return resultList;
    }
}
