package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.tree.BpTreeMap;
import com.moyu.test.store.data.tree.BpTreeStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.type.IntColumnType;
import com.moyu.test.store.type.LongColumnType;
import com.moyu.test.store.type.StringColumnType;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/16
 */
public class InsertCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    private Column[] columns;

    public InsertCommand(Integer databaseId, String tableName, Column[] columns) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.columns = columns;
    }

    @Override
    public String execute() {
        DataChunkStore dataChunkStore = null;
        try {
            // 插入数据
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            Long chunkPos = dataChunkStore.storeRowAndGetPos(columns);
            if(chunkPos == null) {
                return "插入数据失败";
            }
            // 插入主键索引
            Column primaryKeyColumn = getPrimaryKeyColumn(columns);
            if(primaryKeyColumn != null) {
                insertIndex(primaryKeyColumn, chunkPos);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }


    private void insertIndex(Column primaryKeyColumn, Long chunkPos) {
        String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
        String indexPath = dirPath + File.separator + tableName + ".idx";
        FileUtil.createFileIfNotExists(indexPath);
        BpTreeStore bpTreeStore = null;
        try {
            bpTreeStore = new BpTreeStore(indexPath);
            if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.INT.getColumnType()) {
                BpTreeMap<Integer, Long> bTreeMap = new BpTreeMap<>(1024, new IntColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                bTreeMap.put((Integer) primaryKeyColumn.getValue(), chunkPos);
            } else if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
                BpTreeMap<Long, Long> bTreeMap = new BpTreeMap<>(1024, new LongColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                bTreeMap.put((Long) primaryKeyColumn.getValue(), chunkPos);
            } else if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
                BpTreeMap<String, Long> bTreeMap = new BpTreeMap<>(1024, new StringColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                bTreeMap.put((String) primaryKeyColumn.getValue(), chunkPos);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bpTreeStore != null) {
                bpTreeStore.close();
            }
        }
    }


    private Column getPrimaryKeyColumn(Column[] columnArr) {
        for (Column c : columnArr) {
            if (c.getIsPrimaryKey() == (byte) 1) {
                return c;
            }
        }
        return null;
    }


    public String testWriteList(List<Column[]> columnsList) {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            List<byte[]> list = new ArrayList<>();

            for (int i = 0; i < columnsList.size(); i++) {
                Column[] columns = columnsList.get(i);
                byte[] rowBytes = RowData.toRowByteData(columns);
                list.add(rowBytes);
            }
            dataChunkStore.writeRow(list);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }


    public String testSetIndex(Column[] columnArr) {
        DataChunkStore dataChunkStore = null;
        BpTreeStore bpTreeStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();

            BpTreeMap<Integer, Long> bTreeMap = null;
            // 如果索引文件不存在，先创建索引文件
            String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
            String indexPath = dirPath + File.separator + tableName + ".idx";
            FileUtil.createFileIfNotExists(indexPath);

            bpTreeStore = new BpTreeStore(indexPath);
            bTreeMap = new BpTreeMap<>(1024,new IntColumnType(), new LongColumnType(), bpTreeStore, false);
            bTreeMap.initRootNode();

            for (int i = 0; i < dataChunkNum; i++) {
                DataChunk chunk = dataChunkStore.getChunk(i);
                for (int j = 0; j < chunk.getDataRowList().size(); j++) {
                    RowData rowData = chunk.getDataRowList().get(j);
                    Column[] columnData = rowData.getColumnData(columnArr);
                    // 插入主键索引
                    Column primaryKeyColumn = getPrimaryKeyColumn(columnData);
                    if (primaryKeyColumn != null) {
                        bTreeMap.putUnSaveDisk((Integer) primaryKeyColumn.getValue(), chunk.getStartPos());
                    }
                }
            }

            bTreeMap.commitSaveDisk();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }
        return "ok";
    }


    public Column[] getColumns() {
        return columns;
    }
}
