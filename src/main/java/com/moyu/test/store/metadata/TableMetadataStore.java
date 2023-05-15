package com.moyu.test.store.metadata;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.store.metadata.obj.TableMetadata;
import com.moyu.test.util.DataUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/8
 */
public class TableMetadataStore {


    private static final String defaultPath = "D:\\mytest\\fileTest\\meta\\";

    private String filePath;

    public static final String TABLE_META_FILE_NAME = "table.meta";

    private FileStore fileStore;

    private Integer databaseId;

    private List<TableMetadata> tableMetadataList = new ArrayList<>();

    private Map<Integer, List<ColumnMetadata>> columnMap = new HashMap<>();


    public TableMetadataStore(Integer databaseId) throws IOException {
        this(databaseId, defaultPath);
    }


    public TableMetadataStore(Integer databaseId, String filePath) throws IOException {
        this.filePath = filePath;
        this.databaseId = databaseId;
        init();
    }


    public TableMetadata createTable(String tableName) {
        TableMetadata metadata = null;
        synchronized (TableMetadataStore.class) {
            checkTableName(tableName);
            TableMetadata lastData = getLastTable();
            int nextTableId = lastData == null ? 0 : lastData.getDatabaseId() + 1;
            long startPos = lastData == null ? 0L : lastData.getStartPos() + lastData.getTotalByteLen();
            metadata = new TableMetadata(tableName, nextTableId, databaseId, startPos, "NULL");
            ByteBuffer byteBuffer = metadata.getByteBuffer();
            fileStore.write(byteBuffer, startPos);
            tableMetadataList.add(metadata);
        }
        return metadata;
    }


    public void dropTable(String tableName) {
        TableMetadata tableMetadata = null;
        int dropIndex = 0;
        for (int i = 0; i < tableMetadataList.size(); i++) {
            TableMetadata metadata = tableMetadataList.get(i);
            if (tableName.equals(metadata.getTableName())) {
                tableMetadata = metadata;
                dropIndex = i;
                break;
            }
        }

        if(tableMetadata == null) {
            throw new RuntimeException("表" + tableName + "不存在");
        }

        long startPos = tableMetadata.getStartPos();
        long endPos = tableMetadata.getStartPos() + tableMetadata.getTotalByteLen();
        if(endPos >= fileStore.getEndPosition()) {
            fileStore.truncate(startPos);
        } else {
            long oldNextStarPos = endPos;
            while (oldNextStarPos < fileStore.getEndPosition()) {
                int dataByteLen = DataUtils.readInt(fileStore.read(oldNextStarPos, JavaTypeConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileStore.read(oldNextStarPos, dataByteLen);
                fileStore.write(readBuffer, startPos);
                startPos += dataByteLen;
                oldNextStarPos += dataByteLen;
            }
            fileStore.truncate(startPos);
        }

        tableMetadataList.remove(dropIndex);

    }



    public List<TableMetadata> getAllTable() {
        return tableMetadataList;
    }


    public List<ColumnMetadata> getColumnList(Integer tableId) {
        ColumnMetadataStore columnMetadataStore = null;
        try {
            columnMetadataStore = new ColumnMetadataStore(filePath);
            TableColumnBlock columnBlock = columnMetadataStore.getColumnMap().get(tableId);
            if (columnBlock != null) {
                return columnBlock.getColumnMetadataList();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (columnMetadataStore != null) {
                columnMetadataStore.close();
            }
        }
        return new ArrayList<>();
    }


    private void checkTableName(String tableName) {
        for (TableMetadata metadata : tableMetadataList) {
            if (tableName.equals(metadata.getTableName())) {
                throw new RuntimeException("表" + tableName + "已存在");
            }
        }
    }

    private TableMetadata getLastTable() {
        if (tableMetadataList.size() > 0) {
            return tableMetadataList.get(tableMetadataList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {
        // 初始化table的元数据文件，不存在会创建文件，并把所有表信息读取到内存
        String databasePath = filePath + File.separator + TABLE_META_FILE_NAME;
        File dbFile = new File(databasePath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileStore = new FileStore(databasePath);
        long endPosition = fileStore.getEndPosition();
        if (endPosition > JavaTypeConstant.INT_LENGTH) {
            long currPos = 0;
            while (currPos < endPosition) {
                int dataByteLen = DataUtils.readInt(fileStore.read(currPos, JavaTypeConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileStore.read(currPos, dataByteLen);
                TableMetadata dbMetadata = new TableMetadata(readBuffer);
                // 只初始化当前数据库的表到内存
                if(databaseId.equals(dbMetadata.getDatabaseId())) {
                    tableMetadataList.add(dbMetadata);
                }
                currPos += dataByteLen;
            }
        }
    }

    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }




}
