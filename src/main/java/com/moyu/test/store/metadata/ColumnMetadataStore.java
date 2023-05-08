package com.moyu.test.store.metadata;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
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
public class ColumnMetadataStore {


    private static final String defaultPath = "D:\\mytest\\fileTest\\";

    private String filePath;

    private String COLUMN_META_FILE_NAME = "column.meta";

    private FileStore fileStore;

    private Integer tableId;


    private List<ColumnMetadata> columnMetadataList = new ArrayList<>();



    public ColumnMetadataStore(Integer tableId, String filePath) throws IOException {
        this.filePath = filePath;
        this.tableId = tableId;
        init();
    }


    public void createColumn(List<ColumnMetadata> columnMetadata) {
        synchronized (DatabaseMetadataStore.class) {

        }
    }

    public List<ColumnMetadata> getAllTable() {
        return columnMetadataList;
    }


    private void checkColumn(String columnName) {
        for (ColumnMetadata metadata : columnMetadataList) {
            if (columnName.equals(metadata.getColumnName())) {
                throw new RuntimeException("字段" + columnName + "已存在");
            }
        }
    }

    private ColumnMetadata getLastColumn() {
        if (columnMetadataList.size() > 0) {
            return columnMetadataList.get(columnMetadataList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {
        // 初始化table的元数据文件，不存在会创建文件，并把所有表信息读取到内存
        String columnPath = filePath + File.separator + COLUMN_META_FILE_NAME;
        File dbFile = new File(columnPath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileStore = new FileStore(columnPath);
        long endPosition = fileStore.getEndPosition();
        if (endPosition > JavaTypeConstant.INT_LENGTH) {
            long currPos = 0;
            while (currPos < endPosition) {
                int dataByteLen = DataUtils.readInt(fileStore.read(currPos, JavaTypeConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileStore.read(currPos, dataByteLen);
                ColumnMetadata dbMetadata = new ColumnMetadata(readBuffer);
                // 只初始化当前数据库的表到内存
                if(tableId.equals(dbMetadata.getTableId())) {
                    columnMetadataList.add(dbMetadata);
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
