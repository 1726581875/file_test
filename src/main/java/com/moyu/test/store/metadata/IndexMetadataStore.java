package com.moyu.test.store.metadata;

import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.*;
import com.moyu.test.store.metadata.obj.TableIndexBlock;
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
 * @date 2023/5/30
 */
public class IndexMetadataStore {

    private static final String DEFAULT_META_PATH =  PathUtil.getMetaDirPath();

    private String filePath;

    public static final String COLUMN_META_FILE_NAME = "index.meta";

    private FileStore fileStore;


    private List<TableIndexBlock> indexBlockList = new ArrayList<>();

    /**
     * key: tableId
     * value: ColumnIndexBlock
     */
    private Map<Integer, TableIndexBlock> indexBlockMap = new HashMap<>();

    private Integer databaseId;



    public IndexMetadataStore(Integer databaseId) throws IOException {
        this.databaseId = databaseId;
        this.filePath = PathUtil.getIndexMetaPath(databaseId);
        init();
    }


    public void saveIndexMetadata(Integer tableId, IndexMetadata indexMetadata) {
        synchronized (ColumnMetadataStore.class) {
            TableIndexBlock block = indexBlockMap.get(tableId);
            if (block == null) {
                TableIndexBlock lastData = getLastColumnBlock();
                long startPos = lastData == null ? 0 : lastData.getStartPos() + TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
                int blockIndex = lastData == null ? 0 : lastData.getBlockIndex() + 1;
                TableIndexBlock columnBlock = new TableIndexBlock(blockIndex, startPos, tableId);
                indexMetadata.setStartPos(columnBlock.getIndexStartPos());
                columnBlock.addIndex(indexMetadata);
                fileStore.write(columnBlock.getByteBuffer(), startPos);
                indexBlockMap.put(columnBlock.getTableId(), columnBlock);
                indexBlockList.add(columnBlock);
            } else {
                List<IndexMetadata> list = block.getIndexMetadataList();
                if (list != null && list.size() > 0) {
                    IndexMetadata index = list.get(list.size() - 1);
                    indexMetadata.setStartPos(index.getStartPos() + index.getTotalByteLen());
                } else {
                    indexMetadata.setStartPos(block.getIndexStartPos());
                }
                block.addIndex(indexMetadata);
                fileStore.write(block.getByteBuffer(), block.getStartPos());
            }
        }
    }


    public void dropIndexMetadata(Integer tableId, String indexName) {
        synchronized (ColumnMetadataStore.class) {
            TableIndexBlock block = indexBlockMap.get(tableId);
            if (block == null) {
                throw new SqlExecutionException("索引" + indexName + "不存在");
            } else {
                List<IndexMetadata> list = block.getIndexMetadataList();

                int k = -1;
                if (list != null && list.size() > 0) {
                    for (int i = list.size() - 1; i >= 0; i--) {
                        IndexMetadata indexMetadata = list.get(i);
                        if(indexMetadata.getIndexName().equals(indexName)) {
                            k = i;
                            break;
                        }
                    }
                }

                if(k == -1) {
                    throw new SqlExecutionException("索引" + indexName + "不存在");
                } else {
                    list.remove(k);
                }

                // 重新写索引块
                TableIndexBlock newBlock = new TableIndexBlock(block.getBlockIndex(), block.getStartPos(), block.getTableId());
                long startPos = newBlock.getStartPos();
                for (int i = 0; i < list.size(); i++) {
                    IndexMetadata index = list.get(i);
                    index.setStartPos(startPos);
                    newBlock.addIndex(index);
                    startPos += index.getStartPos() + index.getTotalByteLen();
                }
                fileStore.write(newBlock.getByteBuffer(), newBlock.getStartPos());
            }

            try {
                init();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public IndexMetadata getIndex(Integer tableId, String indexName) {
        TableIndexBlock block = indexBlockMap.get(tableId);
        if(block == null) {
            return null;
        }
        List<IndexMetadata> list = block.getIndexMetadataList();
        if (list != null && list.size() > 0) {
            for (int i = list.size() - 1; i >= 0; i--) {
                IndexMetadata indexMetadata = list.get(i);
                if (indexMetadata.getIndexName().equals(indexName)) {
                    return indexMetadata;
                }
            }
        }
        return null;
    }



    public void dropIndexBlock(Integer tableId) {
        TableIndexBlock columnBlock = indexBlockMap.get(tableId);

        if (columnBlock == null) {
            throw new RuntimeException("删除失败，不存在tableId:" + tableId);
        }

        long startPos = columnBlock.getStartPos();
        long endPos = columnBlock.getStartPos() + TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
        if (endPos >= fileStore.getEndPosition()) {
            fileStore.truncate(startPos);
        } else {
            int blockIndex = columnBlock.getBlockIndex();
            long oldNextStarPos = endPos;
            while (oldNextStarPos < fileStore.getEndPosition()) {
                ByteBuffer readBuffer = fileStore.read(oldNextStarPos, TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE);
                TableIndexBlock block = new TableIndexBlock(readBuffer);
                block.setBlockIndex(blockIndex);

                fileStore.write(block.getByteBuffer(), startPos);
                startPos += TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
                oldNextStarPos += TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
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


    public List<TableIndexBlock> getIndexBlockList() {
        return indexBlockList;
    }

    public TableIndexBlock getColumnBlock(Integer tableId) {
        return indexBlockMap.get(tableId);
    }

    public Map<Integer, TableIndexBlock> getIndexMap() {
        return indexBlockMap;
    }


    private TableIndexBlock getLastColumnBlock() {
        if (indexBlockList.size() > 0) {
            return indexBlockList.get(indexBlockList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {
        this.indexBlockList = new ArrayList<>();
        this.indexBlockMap = new HashMap<>();

        // 初始化table的元数据文件，不存在会创建文件，并把所有表信息读取到内存
        String columnPath = filePath + File.separator + COLUMN_META_FILE_NAME;
        File dbFile = new File(columnPath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileStore = new FileStore(columnPath);
        long endPosition = fileStore.getEndPosition();
        if (endPosition >= TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE) {
            long currPos = 0;
            while (currPos < endPosition) {
                ByteBuffer readBuffer = fileStore.read(currPos, TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE);
                TableIndexBlock columnBlock = new TableIndexBlock(readBuffer);
                indexBlockList.add(columnBlock);
                currPos += TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
            }

            for (TableIndexBlock columnBlock : indexBlockList) {
                indexBlockMap.put(columnBlock.getTableId(), columnBlock);
            }
        }
    }

    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }

}
