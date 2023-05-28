package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.data.tree.BTreeMap;
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
            Column primaryKeyColumn = getPrimaryKeyColumn();
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
                BTreeMap<Integer, Long> bTreeMap = new BTreeMap<>(1024, new IntColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                bTreeMap.put((Integer) primaryKeyColumn.getValue(), chunkPos);
            } else if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.BIGINT.getColumnType()) {
                BTreeMap<Long, Long> bTreeMap = new BTreeMap<>(1024, new LongColumnType(), new LongColumnType(), bpTreeStore);
                bTreeMap.initRootNode();
                bTreeMap.put((Long) primaryKeyColumn.getValue(), chunkPos);
            } else if (primaryKeyColumn.getColumnType() == ColumnTypeEnum.VARCHAR.getColumnType()) {
                BTreeMap<String, Long> bTreeMap = new BTreeMap<>(1024, new StringColumnType(), new LongColumnType(), bpTreeStore);
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


    private Column getPrimaryKeyColumn() {
        for (Column c : columns) {
            if (c.getIsPrimaryKey() == (byte) 1) {
                return c;
            }
        }
        return null;
    }


    public String testWriteList() {
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            List<byte[]> list = new ArrayList<>();
            byte[] rowBytes = RowData.toRowByteData(columns);
            for (int i = 0; i < 1024; i++) {
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

}
