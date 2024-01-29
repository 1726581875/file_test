package com.moyu.xmz.store.accessor;

import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMetadata;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.util.PathUtil;

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
public class ColumnMetaFileAccessor {

    private static final String DEFAULT_META_PATH =  PathUtil.getMetaDirPath();

    public static final String COLUMN_META_FILE_NAME = "column.meta";

    private FileAccessor fileAccessor;

    private Integer databaseId;

    private String filePath;


    private List<TableColumnBlock> columnBlockList = new ArrayList<>();

    /**
     * key: tableId
     * value: TableColumnBlock
     */
    private Map<Integer, TableColumnBlock> columnBlockMap = new HashMap<>();

    public ColumnMetaFileAccessor(Integer databaseId) throws IOException {
        this(DEFAULT_META_PATH + File.separator + databaseId);
    }

    public ColumnMetaFileAccessor(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }


    public void createColumnBlock(Integer tableId,
                                  List<Column> columnDtoList) {
        synchronized (ColumnMetaFileAccessor.class) {
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
                column.setIsNotNull(columnDto.getIsNotNull());
                column.setDefaultVal(columnDto.getDefaultVal());
                column.setComment(columnDto.getComment());
                columnBlock.addColumn(column);
                columnStartPos += column.getTotalByteLen();
            }
            fileAccessor.write(columnBlock.getByteBuffer(), startPos);

            columnBlockMap.put(columnBlock.getTableId(), columnBlock);
            columnBlockList.add(columnBlock);
        }
    }


    public void dropColumnBlock(Integer tableId) {

        synchronized (ColumnMetaFileAccessor.class) {
            TableColumnBlock columnBlock = columnBlockMap.get(tableId);

            if (columnBlock == null) {
                ExceptionUtil.throwSqlExecutionException("删除失败，表id={}对应的字段块不存在", tableId);
                throw new RuntimeException("删除失败，不存在tableId:" + tableId);
            }

            long startPos = columnBlock.getStartPos();
            long endPos = columnBlock.getStartPos() + TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
            if (endPos >= fileAccessor.getEndPosition()) {
                fileAccessor.truncate(startPos);
            } else {
                int blockIndex = columnBlock.getBlockIndex();
                long oldNextStarPos = endPos;
                while (oldNextStarPos < fileAccessor.getEndPosition()) {
                    ByteBuffer readBuffer = fileAccessor.read(oldNextStarPos, TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE);
                    TableColumnBlock block = new TableColumnBlock(readBuffer);
                    block.setBlockIndex(blockIndex);
                    block.setStartPos(startPos);
                    fileAccessor.write(block.getByteBuffer(), startPos);
                    startPos += TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
                    oldNextStarPos += TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE;
                    blockIndex++;
                }
                fileAccessor.truncate(startPos);
            }

            try {
                init();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("init error");
            }
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
        fileAccessor = new FileAccessor(columnPath);
        long endPosition = fileAccessor.getEndPosition();
        if (endPosition >= TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE) {
            long currPos = 0;
            while (currPos < endPosition) {
                ByteBuffer readBuffer = fileAccessor.read(currPos, TableColumnBlock.TABLE_COLUMN_BLOCK_SIZE);
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
        if (fileAccessor != null) {
            fileAccessor.close();
        }
    }


}
