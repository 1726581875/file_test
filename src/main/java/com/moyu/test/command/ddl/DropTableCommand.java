package com.moyu.test.command.ddl;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.TableMetadata;

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
            // TODO 删除数据

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
