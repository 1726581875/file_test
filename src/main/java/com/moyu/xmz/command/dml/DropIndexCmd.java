package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.common.util.FileUtils;
import com.moyu.xmz.common.util.PathUtils;

import java.io.File;

/**
 * @author xiaomingzhang
 * @date 2023/6/1
 */
public class DropIndexCmd extends AbstractCmd {

    private Integer databaseId;

    private Integer tableId;

    private String tableName;

    private String indexName;


    public DropIndexCmd(Integer databaseId, Integer tableId, String tableName, String indexName) {
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public QueryResult execCommand() {
        IndexMetaAccessor indexStore = null;
        try {
            indexStore = new IndexMetaAccessor(databaseId);
            indexStore.dropIndexMetadata(this.tableId, this.indexName);

            // 索引路径
            String dirPath = PathUtils.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
            // 删除索引文件
            FileUtils.deleteOnExists(indexPath);
        } catch (Exception e) {
            e.printStackTrace();
            return QueryResult.simpleResult(RESULT_ERROR);
        } finally {
            if(indexStore != null) {
                indexStore.close();
            }
        }
        return QueryResult.simpleResult(RESULT_OK);
    }

}
