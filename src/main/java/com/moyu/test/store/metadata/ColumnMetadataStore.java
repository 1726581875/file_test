package com.moyu.test.store.metadata;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
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
public class ColumnMetadataStore {


    private static final String defaultPath = "D:\\mytest\\fileTest\\meta\\";

    private String filePath;

    public static final String COLUMN_META_FILE_NAME = "column.meta";

    private FileStore fileStore;


    private List<ColumnMetadata> columnMetadataList = new ArrayList<>();

    private Map<Integer, List<ColumnMetadata>> columnMap = new HashMap<>();


    public ColumnMetadataStore() throws IOException {
        this(defaultPath);
    }

    public ColumnMetadataStore(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }


    public void createColumn(Integer tableId, List<Column> columnDtoList) {
        synchronized (ColumnMetadataStore.class) {
            ColumnMetadata lastData = getLastColumn();
            long startPos = lastData == null ? 0 : lastData.getStartPos() + lastData.getTotalByteLen();
            for (int i = 0; i < columnDtoList.size(); i++) {
                // 写入磁盘文件
                Column columnDto = columnDtoList.get(i);
                ColumnMetadata metadata = new ColumnMetadata(tableId, startPos, columnDto.getColumnName(),
                        columnDto.getColumnType(), columnDto.getColumnIndex(), columnDto.getColumnLength());
                ByteBuffer byteBuffer = metadata.getByteBuffer();
                fileStore.write(byteBuffer, startPos);
                startPos += metadata.getTotalByteLen();

                // 添加到内存
                columnMetadataList.add(metadata);
                List<ColumnMetadata> columns = columnMap.get(tableId);
                if (columns == null) {
                    columns = new ArrayList<>();
                    columnMap.put(tableId, columns);
                }
                columns.add(metadata);

            }
        }
    }

    public List<ColumnMetadata> getAllTable() {
        return columnMetadataList;
    }

    public Map<Integer, List<ColumnMetadata>> getColumnMap() {
        return columnMap;
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
                columnMetadataList.add(dbMetadata);
                currPos += dataByteLen;
            }

            for (ColumnMetadata metadata : columnMetadataList) {
                List<ColumnMetadata> columns = columnMap.get(metadata.getTableId());
                if (columns == null) {
                    columns = new ArrayList<>();
                    columnMap.put(metadata.getTableId(), columns);
                }
                columns.add(metadata);
            }
        }
    }

    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }


}
