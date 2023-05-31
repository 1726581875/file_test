package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.ColumnTypeEnum;
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

    private Column[] columns;

    private Column indexColumn;


    @Override
    public String execute() {
        DataChunkStore dataChunkStore = null;
        IndexMetadataStore indexMetadataStore = null;
        try {
            // 保存索引元数据
            indexMetadataStore = new IndexMetadataStore();
            IndexMetadata index = new IndexMetadata(0L, tableId, indexName, columnName, (byte) 0);
            indexMetadataStore.saveIndexMetadata(tableId, index);

            // 生成创建索引文件
            String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName +"_" + indexName +".idx";

            File file = new File(indexPath);
            if(file.exists()) {
                file.delete();
            }
            FileUtil.createFileIfNotExists(indexPath);

            // 索引为int类型
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
            if (indexColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
                BpTreeMap<Integer, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Integer.class);
                bpTreeMap.initRootNode();
                for (int i = 0; i < dataChunkNum; i++) {
                    DataChunk chunk = dataChunkStore.getChunk(i);
                    for (int j = 0; j < chunk.getDataRowList().size(); j++) {
                        RowData rowData = chunk.getDataRowList().get(j);
                        Column[] columnData = rowData.getColumnData(columns);
                        // 找到对应字段插入索引
                        Column indexColumnValue = getIndexColumn(columnData);
                        if (indexColumnValue != null) {
                            Integer key = (Integer) indexColumnValue.getValue();
                            Long[] arr = bpTreeMap.get(key);
                            Long[] saveArr = insertPosArr(arr, chunk.getStartPos());
                            bpTreeMap.putUnSaveDisk(key, saveArr);
                        }
                    }
                }
                bpTreeMap.commitSaveDisk();

                // 索引为Long类型
            } else if (indexColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
                BpTreeMap<Long, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, Long.class);
                bpTreeMap.initRootNode();
                for (int i = 0; i < dataChunkNum; i++) {
                    DataChunk chunk = dataChunkStore.getChunk(i);
                    for (int j = 0; j < chunk.getDataRowList().size(); j++) {
                        RowData rowData = chunk.getDataRowList().get(j);
                        Column[] columnData = rowData.getColumnData(columns);
                        Column indexColumnValue = getIndexColumn(columnData);
                        if (indexColumnValue != null) {
                            Long key = (Long) indexColumnValue.getValue();
                            Long[] arr = bpTreeMap.get(key);
                            Long[] saveArr = insertPosArr(arr, chunk.getStartPos());
                            bpTreeMap.putUnSaveDisk(key, saveArr);
                        }
                    }
                }
                bpTreeMap.commitSaveDisk();

                // 索引为String类型
            } else if (indexColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
                BpTreeMap<String, Long[]> bpTreeMap = BpTreeMap.getBpTreeMap(indexPath, true, String.class);
                bpTreeMap.initRootNode();
                for (int i = 0; i < dataChunkNum; i++) {
                    DataChunk chunk = dataChunkStore.getChunk(i);
                    for (int j = 0; j < chunk.getDataRowList().size(); j++) {
                        RowData rowData = chunk.getDataRowList().get(j);
                        Column[] columnData = rowData.getColumnData(columns);
                        // 找到对应字段插入索引
                        Column indexColumnValue = getIndexColumn(columnData);
                        if (indexColumnValue != null) {
                            String key = (String) indexColumnValue.getValue();
                            Long[] arr = bpTreeMap.get(key);
                            Long[] saveArr = insertPosArr(arr, chunk.getStartPos());
                            bpTreeMap.putUnSaveDisk(key, saveArr);
                        }
                    }
                }
                bpTreeMap.commitSaveDisk();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "error";
        } finally {
            dataChunkStore.close();
            indexMetadataStore.close();
        }
        return "ok";
    }


    private Long[] insertPosArr(Long[] arr, Long pos) {
        if (arr == null) {
            arr = new Long[1];
            arr[0] = pos;
            return arr;
        } else {
            for (Long p : arr) {
                if (p.equals(pos)) {
                    return arr;
                }
            }

            Long[] newArr = new Long[arr.length + 1];
            System.arraycopy(arr, 0, newArr, 0, arr.length);
            newArr[newArr.length - 1] = pos;
            return newArr;
        }
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
}
