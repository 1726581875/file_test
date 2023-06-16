package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.store.metadata.obj.TableIndexBlock;
import com.moyu.test.store.metadata.obj.TableMetadata;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class DropTableCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    private boolean ifExists;


    public DropTableCommand(Integer databaseId, String tableName, boolean ifExists) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    @Override
    public String execute() {

        boolean result;
        TableMetadataStore tableMetadataStore = null;
        ColumnMetadataStore columnMetadataStore = null;
        IndexMetadataStore indexStore = null;
        try {
            tableMetadataStore = new TableMetadataStore(databaseId);
            columnMetadataStore = new ColumnMetadataStore();
            indexStore = new IndexMetadataStore();

            TableMetadata table = tableMetadataStore.getTable(tableName);
            if (table != null) {
                // 删除表元数据
                TableMetadata tableMetadata = tableMetadataStore.dropTable(tableName);
                // 删除字段元数据
                columnMetadataStore.dropColumnBlock(tableMetadata.getTableId());
                // 删除数据文件
                String dataFilePath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
                FileUtil.deleteOnExists(dataFilePath);
                // 存在索引，则删除索引
                TableIndexBlock columnBlock = indexStore.getColumnBlock(tableMetadata.getTableId());
                if (columnBlock != null) {
                    // 删除索引元数据
                    indexStore.dropIndexBlock(tableMetadata.getTableId());
                    // 删除索引文件
                    List<IndexMetadata> indexMetadataList = columnBlock.getIndexMetadataList();
                    for (IndexMetadata index : indexMetadataList) {
                        String indexFilePath = PathUtil.getIndexFilePath(this.databaseId, this.tableName, index.getIndexName());
                        FileUtil.deleteOnExists(indexFilePath);
                    }
                }
            } else if (table == null && !ifExists) {
                throw new SqlExecutionException("表" + this.tableName + "不存在");
            }
            result = true;
        } catch (Exception e) {
            result = false;
            e.printStackTrace();
        } finally {
            tableMetadataStore.close();
            columnMetadataStore.close();
            indexStore.close();
        }
        return result ? "ok" : "error";
    }
}
