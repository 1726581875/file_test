package com.moyu.test.store.metadata;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableMetadata;
import com.moyu.test.util.DataUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/8
 */
public class TableMetadataStore {


    private static final String defaultPath = "D:\\mytest\\fileTest\\";

    private String filePath;

    private String TABLE_META_FILE_NAME = "table.meta";

    private FileStore fileStore;

    private Integer databaseId;

    private List<TableMetadata> tableMetadataList = new ArrayList<>();

    private List<ColumnMetadata> columnMetadataList = new ArrayList<>();



    public TableMetadataStore(Integer databaseId, String filePath) throws IOException {
        this.filePath = filePath;
        this.databaseId = databaseId;
        init();
    }


    public void createTable(String tableName) {
        synchronized (TableMetadataStore.class) {
            synchronized (DatabaseMetadataStore.class) {
                checkTableName(tableName);
                TableMetadata metadata = null;
                TableMetadata lastData = getLastTable();

                int nextTableId = 0;
                long startPos = 0L;
                if (lastData == null) {
                    metadata = new TableMetadata(tableName, nextTableId,databaseId, startPos, "NULL");
                } else {
                    nextTableId = lastData.getDatabaseId() + 1;
                    startPos = lastData.getStartPos() + lastData.getTotalByteLen();
                    metadata = new TableMetadata(tableName, nextTableId, databaseId, startPos, "NULL");;
                }

                ByteBuffer byteBuffer = metadata.getByteBuffer();
                fileStore.write(byteBuffer, startPos);
                tableMetadataList.add(metadata);
            }
        }
    }

    public List<TableMetadata> getAllTable() {
        return tableMetadataList;
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
