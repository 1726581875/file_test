package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.accessor.DatabaseMetaFileAccessor;
import com.moyu.xmz.store.common.meta.DatabaseMetadata;
import com.moyu.xmz.store.common.dto.SelectColumn;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class ShowDatabasesCommand extends AbstractCommand {

    private List<DatabaseMetadata> resultList;

    @Override
    public QueryResult execCommand() {

        long queryStartTime = System.currentTimeMillis();

        SelectColumn idColumn = SelectColumn.newColumn("id", ColumnTypeConstant.INT_4);
        SelectColumn nameColumn = SelectColumn.newColumn("dbName", ColumnTypeConstant.CHAR);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{idColumn, nameColumn});
        DatabaseMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaFileAccessor();
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

        long queryEndTime = System.currentTimeMillis();
        String desc = "查询结果行数:" + queryResult.getResultRows().size() + ", 耗时:" + (queryEndTime - queryStartTime) + "ms";
        queryResult.setDesc(desc);

        return queryResult;
    }

    public List<DatabaseMetadata> getResultList() {
        return resultList;
    }
}
