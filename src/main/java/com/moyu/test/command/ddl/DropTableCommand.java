package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.TableMetadata;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class DropTableCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;


    public DropTableCommand(Integer databaseId, String tableName) {
        this.databaseId = databaseId;
        this.tableName = tableName;
    }

    @Override
    public String execute() {

        boolean result = false;
        TableMetadataStore tableMetadataStore = null;
        ColumnMetadataStore columnMetadataStore = null;
        try{
            tableMetadataStore = new TableMetadataStore(databaseId);
            columnMetadataStore = new ColumnMetadataStore();
            // 删除表元数据
            TableMetadata tableMetadata = tableMetadataStore.dropTable(tableName);
            // 删除字段元数据
            columnMetadataStore.dropColumnBlock(tableMetadata.getTableId());
            // 删除数据文件
            String dataFilePath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            FileUtil.deleteOnExists(dataFilePath);

            result = true;
        } catch (Exception e){
            result = false;
            e.printStackTrace();
        } finally {
            tableMetadataStore.close();
            columnMetadataStore.close();
        }
        return result ? "ok" : "error";
    }
}
