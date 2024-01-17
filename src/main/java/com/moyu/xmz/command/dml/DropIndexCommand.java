package com.moyu.xmz.command.dml;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.common.util.FileUtil;
import com.moyu.xmz.common.util.PathUtil;

import java.io.File;

/**
 * @author xiaomingzhang
 * @date 2023/6/1
 */
public class DropIndexCommand extends AbstractCommand {

    private Integer databaseId;

    private Integer tableId;

    private String tableName;

    private String indexName;


    public DropIndexCommand(Integer databaseId, Integer tableId, String tableName, String indexName) {
        this.databaseId = databaseId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public QueryResult execCommand() {
        IndexMetaFileAccessor indexStore = null;
        try {
            indexStore = new IndexMetaFileAccessor(databaseId);
            indexStore.dropIndexMetadata(this.tableId, this.indexName);

            // 索引路径
            String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
            // 删除索引文件
            FileUtil.deleteOnExists(indexPath);
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
