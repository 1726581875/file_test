package com.moyu.test.store.metadata;

import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.util.PathUtil;

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

    private static final String DEFAULT_META_PATH =  PathUtil.getMetaDirPath();

    private String filePath;

    public static final String COLUMN_META_FILE_NAME = "column.meta";

    private FileStore fileStore;


    private List<TableColumnBlock> columnBlockList = new ArrayList<>();

    /**
     * key: tableId
     * value: TableColumnBlock
     */
    private Map<Integer, TableColumnBlock> columnBlockMap = new HashMap<>();


    public ColumnMetadataStore() throws IOException {
        this(DEFAULT_META_PATH);
    }

    public ColumnMetadataStore(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }


    public void createColumnBlock(Integer tableId, List<Column> columnDtoList) {
        synchronized (ColumnMetadataStore.class) {
            TableColumnBlock lastData = getLastColumnBlock();
            long startPos = lastData == null ? 0 : lastData.getStartPos() + TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
            int blockIndex = lastData == null ? 0 : lastData.getBlockIndex() + 1;

            TableColumnBlock columnBlock = new TableColumnBlock(blockIndex, startPos, tableId);
            long columnStartPos = columnBlock.getStartPos();
            for (int i = 0; i < columnDtoList.size(); i++) {
                Column columnDto = columnDtoList.get(i);
                ColumnMetadata column = new ColumnMetadata(tableId, columnStartPos, columnDto.getColumnName(),
                        columnDto.getColumnType(), columnDto.getColumnIndex(), columnDto.getColumnLength());
                column.setIsPrimaryKey(columnDto.getIsPrimaryKey());
                columnBlock.addColumn(column);
                columnStartPos += column.getTotalByteLen();
            }
            fileStore.write(columnBlock.getByteBuffer(), startPos);

            columnBlockMap.put(columnBlock.getTableId(), columnBlock);
            columnBlockList.add(columnBlock);
        }
    }


    public void dropColumnBlock(Integer tableId) {
        TableColumnBlock columnBlock = columnBlockMap.get(tableId);

        if (columnBlock == null) {
            throw new RuntimeException("删除失败，不存在tableId:" + tableId);
        }

        long startPos = columnBlock.getStartPos();
        long endPos = columnBlock.getStartPos() + TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
        if (endPos >= fileStore.getEndPosition()) {
            fileStore.truncate(startPos);
        } else {
            int blockIndex = columnBlock.getBlockIndex();
            long oldNextStarPos = endPos;
            while (oldNextStarPos < fileStore.getEndPosition()) {
                ByteBuffer readBuffer = fileStore.read(oldNextStarPos, TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE);
                TableColumnBlock block = new TableColumnBlock(readBuffer);
                block.setBlockIndex(blockIndex);

                fileStore.write(block.getByteBuffer(), startPos);
                startPos += TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
                oldNextStarPos += TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
                blockIndex++;
            }
            fileStore.truncate(startPos);
        }

        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("init error");
        }

    }




    public List<TableColumnBlock> getAllColumnBlock() {
        return columnBlockList;
    }

    public TableColumnBlock getColumnBlock(Integer tableId) {
        return columnBlockMap.get(tableId);
    }

    public Map<Integer, TableColumnBlock> getColumnMap() {
        return columnBlockMap;
    }


    private TableColumnBlock getLastColumnBlock() {
        if (columnBlockList.size() > 0) {
            return columnBlockList.get(columnBlockList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {
        this.columnBlockList = new ArrayList<>();
        this.columnBlockMap = new HashMap<>();

        // 初始化table的元数据文件，不存在会创建文件，并把所有表信息读取到内存
        String columnPath = filePath + File.separator + COLUMN_META_FILE_NAME;
        File dbFile = new File(columnPath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileStore = new FileStore(columnPath);
        long endPosition = fileStore.getEndPosition();
        if (endPosition >= TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE) {
            long currPos = 0;
            while (currPos < endPosition) {
                ByteBuffer readBuffer = fileStore.read(currPos, TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE);
                TableColumnBlock columnBlock = new TableColumnBlock(readBuffer);
                columnBlockList.add(columnBlock);
                currPos += TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
            }

            for (TableColumnBlock columnBlock : columnBlockList) {
                columnBlockMap.put(columnBlock.getTableId(), columnBlock);
            }
        }
    }

    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }


}
