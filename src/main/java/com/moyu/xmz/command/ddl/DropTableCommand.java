package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCommand;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.accessor.ColumnMetaFileAccessor;
import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.store.common.meta.TableMetadata;
import com.moyu.xmz.common.util.FileUtil;
import com.moyu.xmz.common.util.PathUtil;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class DropTableCommand extends AbstractCommand {

    private Database database;

    private String tableName;

    private boolean ifExists;


    public DropTableCommand(Database database, String tableName, boolean ifExists) {
        this.database = database;
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    @Override
    public QueryResult execCommand() {
        boolean result;
        TableMetaFileAccessor tableMetaFileAccessor = null;
        ColumnMetaFileAccessor columnMetaFileAccessor = null;
        IndexMetaFileAccessor indexStore = null;
        try {
            tableMetaFileAccessor = new TableMetaFileAccessor(database.getDatabaseId());
            columnMetaFileAccessor = new ColumnMetaFileAccessor(database.getDatabaseId());
            indexStore = new IndexMetaFileAccessor(database.getDatabaseId());
            TableMetadata table = tableMetaFileAccessor.getTable(tableName);
            if (table != null) {
                // 删除表元数据
                TableMetadata tableMetadata = tableMetaFileAccessor.dropTable(tableName);
                // 删除字段元数据
                columnMetaFileAccessor.dropColumnBlock(tableMetadata.getTableId());
                // 删除数据文件
                String dataFilePath = null;
                if(CommonConstant.ENGINE_TYPE_YAN.equals(tableMetadata.getEngineType())) {
                    dataFilePath = PathUtil.getYanEngineDataFilePath(database.getDatabaseId(), this.tableName);
                } else {
                    dataFilePath = PathUtil.getDataFilePath(database.getDatabaseId(), this.tableName);
                }
                FileUtil.deleteOnExists(dataFilePath);
                // 存在索引，则删除索引
                TableIndexBlock columnBlock = indexStore.getColumnBlock(tableMetadata.getTableId());
                if (columnBlock != null) {
                    // 删除索引元数据
                    indexStore.dropIndexBlock(tableMetadata.getTableId());
                    // 删除索引文件
                    List<IndexMetadata> indexMetadataList = columnBlock.getIndexMetadataList();
                    for (IndexMetadata index : indexMetadataList) {
                        String indexFilePath = PathUtil.getIndexFilePath(database.getDatabaseId(), this.tableName, index.getIndexName());
                        FileUtil.deleteOnExists(indexFilePath);
                    }
                }

                database.removeTable(tableName);

            } else if (table == null && !ifExists) {
                ExceptionUtil.throwSqlExecutionException("表{}不存在", this.tableName);
            }
            result = true;
        } catch (Exception e) {
            result = false;
            e.printStackTrace();
        } finally {
            tableMetaFileAccessor.close();
            columnMetaFileAccessor.close();
            indexStore.close();
        }
        return result ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }
}
