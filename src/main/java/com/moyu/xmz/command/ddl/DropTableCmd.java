package com.moyu.xmz.command.ddl;

import com.moyu.xmz.command.AbstractCmd;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;
import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.store.common.meta.TableMeta;
import com.moyu.xmz.common.util.FileUtils;
import com.moyu.xmz.common.util.PathUtils;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class DropTableCmd extends AbstractCmd {

    private Database database;

    private String tableName;

    private boolean ifExists;


    public DropTableCmd(Database database, String tableName, boolean ifExists) {
        this.database = database;
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    @Override
    public QueryResult exec() {
        boolean result;
        TableMetaAccessor tableMetaAccessor = null;
        ColumnMetaAccessor columnMetaAccessor = null;
        IndexMetaAccessor indexStore = null;
        try {
            tableMetaAccessor = new TableMetaAccessor(database.getDatabaseId());
            columnMetaAccessor = new ColumnMetaAccessor(database.getDatabaseId());
            indexStore = new IndexMetaAccessor(database.getDatabaseId());
            TableMeta table = tableMetaAccessor.getTable(tableName);
            if (table != null) {
                // 删除表元数据
                TableMeta tableMeta = tableMetaAccessor.dropTable(tableName);
                // 删除字段元数据
                columnMetaAccessor.dropColumnBlock(tableMeta.getTableId());
                // 删除数据文件
                String dataFilePath = null;
                if(CommonConstant.ENGINE_TYPE_YAN.equals(tableMeta.getEngineType())) {
                    dataFilePath = PathUtils.getYanEngineDataFilePath(database.getDatabaseId(), this.tableName);
                } else {
                    dataFilePath = PathUtils.getDataFilePath(database.getDatabaseId(), this.tableName);
                }
                FileUtils.deleteOnExists(dataFilePath);
                // 存在索引，则删除索引
                TableIndexBlock columnBlock = indexStore.getColumnBlock(tableMeta.getTableId());
                if (columnBlock != null) {
                    // 删除索引元数据
                    indexStore.dropIndexBlock(tableMeta.getTableId());
                    // 删除索引文件
                    List<IndexMeta> indexMetaList = columnBlock.getIndexMetaList();
                    for (IndexMeta index : indexMetaList) {
                        String indexFilePath = PathUtils.getIndexFilePath(database.getDatabaseId(), this.tableName, index.getIndexName());
                        FileUtils.deleteOnExists(indexFilePath);
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
            tableMetaAccessor.close();
            columnMetaAccessor.close();
            indexStore.close();
        }
        return result ? QueryResult.simpleResult(RESULT_OK) : QueryResult.simpleResult(RESULT_ERROR);
    }
}
