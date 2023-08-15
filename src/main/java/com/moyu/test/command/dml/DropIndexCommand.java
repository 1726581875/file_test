package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

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
    public String execute() {
        IndexMetadataStore indexStore = null;
        try {
            indexStore = new IndexMetadataStore(databaseId);
            indexStore.dropIndexMetadata(this.tableId, this.indexName);

            // 索引路径
            String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
            // 删除索引文件
            FileUtil.deleteOnExists(indexPath);
        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        } finally {
            if(indexStore != null) {
                indexStore.close();
            }
        }
        return "ok";
    }


}
