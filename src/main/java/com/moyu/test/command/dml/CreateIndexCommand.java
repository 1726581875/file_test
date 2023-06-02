package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.io.File;

/**
 * @author xiaomingzhang
 * @date 2023/5/31
 */
public class CreateIndexCommand extends AbstractCommand {


    private Integer databaseId;

    private Integer tableId;

    private String tableName;

    private String columnName;

    private String indexName;

    private Byte indexType;

    private Column[] columns;

    private Column indexColumn;


    @Override
    public String execute() {
        DataChunkStore dataChunkStore = null;
        IndexMetadataStore indexStore = null;
        try {
            indexStore = new IndexMetadataStore();

            IndexMetadata oldIndex = indexStore.getIndex(this.tableId, this.indexName);
            // 存在则先删除索引元数据
            if (oldIndex != null) {
                indexStore.dropIndexMetadata(this.tableId, this.indexName);
            }

            // 保存索引元数据
            IndexMetadata index = new IndexMetadata(0L, tableId, indexName, columnName, indexType);
            indexStore.saveIndexMetadata(tableId, index);

            // 索引路径
            String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
            // 索引文件存在则先删除
            File file = new File(indexPath);
            if (file.exists()) {
                file.delete();
            }
            // 创建索引文件
            FileUtil.createFileIfNotExists(indexPath);

            // 操作数据文件，获取数据块数量(后面遍历没一行数据，为每一行数据创建索引)
            dataChunkStore = new DataChunkStore(PathUtil.getDataFilePath(this.databaseId, this.tableName));
            int dataChunkNum = dataChunkStore.getDataChunkNum();

            // 根据索引类型构造索引
            if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
                buildIndexTree(indexPath, dataChunkNum, dataChunkStore, Integer.class);
            } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
                buildIndexTree(indexPath, dataChunkNum, dataChunkStore, Long.class);
            } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
                buildIndexTree(indexPath, dataChunkNum, dataChunkStore, String.class);
            } else {
                throw new SqlExecutionException("该字段类型不支持创建索引，类型:" + indexColumn.getColumnType());
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        } finally {
            dataChunkStore.close();
            indexStore.close();
        }
        return "ok";
    }


    private <T extends Comparable> void buildIndexTree(String indexPath, Integer dataChunkNum, DataChunkStore dataChunkStore, Class<T> clazz) {
        // 创建一颗b+树
        BpTreeMap<T, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, false, clazz);
        // 一行一行遍历数据
        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            for (int j = 0; j < chunk.getDataRowList().size(); j++) {
                RowData rowData = chunk.getDataRowList().get(j);
                Column[] columnData = rowData.getColumnData(columns);
                // 找到对应索引字段
                Column indexColumnValue = getIndexColumn(columnData);
                if (indexColumnValue != null) {
                    // 当前行对应索引字段的值作为b+树的【关键字】，数据块位置作为b+数的【值】
                    T key = (T) indexColumnValue.getValue();
                    Long[] arr = bpTreeMap.get(key);
                    Long[] valueArr = BpTreeMap.insertValueArray(arr, chunk.getStartPos());
                    bpTreeMap.putUnSaveDisk(key, valueArr);
                }
            }
        }
        // 保存到磁盘
        bpTreeMap.commitSaveDisk();
    }


    private Column getIndexColumn(Column[] columnArr) {
        for (Column c : columnArr) {
            if (indexColumn.getColumnName().equals(c.getColumnName())) {
                return c;
            }
        }
        return null;
    }


    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    public Column getIndexColumn() {
        return indexColumn;
    }

    public void setIndexColumn(Column indexColumn) {
        this.indexColumn = indexColumn;
    }

    public Byte getIndexType() {
        return indexType;
    }

    public void setIndexType(Byte indexType) {
        this.indexType = indexType;
    }
}
