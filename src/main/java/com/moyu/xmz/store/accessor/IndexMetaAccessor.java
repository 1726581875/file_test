package com.moyu.xmz.store.accessor;

import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.common.util.PathUtils;

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
public class IndexMetaAccessor extends BaseAccessor {

    public static final String FILE_NAME = "index.meta";

    private List<TableIndexBlock> indexBlockList = new ArrayList<>();

    /**
     * key: tableId
     * value: ColumnIndexBlock
     */
    private Map<Integer, TableIndexBlock> indexBlockMap = new HashMap<>();

    private Integer databaseId;



    public IndexMetaAccessor(Integer databaseId) throws IOException {
        super(PathUtils.getDbMetaBasePath(databaseId) + File.separator + FILE_NAME);
        this.databaseId = databaseId;
        init();
    }


    public void saveIndexMetadata(Integer tableId, IndexMeta indexMeta) {
        synchronized (ColumnMetaAccessor.class) {
            TableIndexBlock block = indexBlockMap.get(tableId);
            if (block == null) {
                TableIndexBlock lastData = getLastColumnBlock();
                long startPos = lastData == null ? 0 : lastData.getStartPos() + TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
                int blockIndex = lastData == null ? 0 : lastData.getBlockIndex() + 1;
                TableIndexBlock columnBlock = new TableIndexBlock(blockIndex, startPos, tableId);
                indexMeta.setStartPos(columnBlock.getIndexStartPos());
                columnBlock.addIndex(indexMeta);
                fileAccessor.write(columnBlock.getByteBuffer(), startPos);
                indexBlockMap.put(columnBlock.getTableId(), columnBlock);
                indexBlockList.add(columnBlock);
            } else {
                List<IndexMeta> list = block.getIndexMetaList();
                if (list != null && list.size() > 0) {
                    IndexMeta index = list.get(list.size() - 1);
                    indexMeta.setStartPos(index.getStartPos() + index.getTotalByteLen());
                } else {
                    indexMeta.setStartPos(block.getIndexStartPos());
                }
                block.addIndex(indexMeta);
                fileAccessor.write(block.getByteBuffer(), block.getStartPos());
            }
        }
    }


    public void dropIndexMetadata(Integer tableId, String indexName) {
        synchronized (ColumnMetaAccessor.class) {
            TableIndexBlock block = indexBlockMap.get(tableId);
            if (block == null) {
                ExceptionUtil.throwSqlExecutionException("索引{}不存在", indexName);
            } else {
                List<IndexMeta> list = block.getIndexMetaList();

                int k = -1;
                if (list != null && list.size() > 0) {
                    for (int i = list.size() - 1; i >= 0; i--) {
                        IndexMeta indexMeta = list.get(i);
                        if(indexMeta.getIndexName().equals(indexName)) {
                            k = i;
                            break;
                        }
                    }
                }

                if(k == -1) {
                    ExceptionUtil.throwSqlExecutionException("索引{}不存在", indexName);
                } else {
                    list.remove(k);
                }

                // 重新写索引块
                TableIndexBlock newBlock = new TableIndexBlock(block.getBlockIndex(), block.getStartPos(), block.getTableId());
                long startPos = newBlock.getStartPos();
                for (int i = 0; i < list.size(); i++) {
                    IndexMeta index = list.get(i);
                    index.setStartPos(startPos);
                    newBlock.addIndex(index);
                    startPos += index.getStartPos() + index.getTotalByteLen();
                }
                fileAccessor.write(newBlock.getByteBuffer(), newBlock.getStartPos());
            }

            try {
                init();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public IndexMeta getIndex(Integer tableId, String indexName) {
        TableIndexBlock block = indexBlockMap.get(tableId);
        if(block == null) {
            return null;
        }
        List<IndexMeta> list = block.getIndexMetaList();
        if (list != null && list.size() > 0) {
            for (int i = list.size() - 1; i >= 0; i--) {
                IndexMeta indexMeta = list.get(i);
                if (indexMeta.getIndexName().equals(indexName)) {
                    return indexMeta;
                }
            }
        }
        return null;
    }



    public void dropIndexBlock(Integer tableId) {
        TableIndexBlock columnBlock = indexBlockMap.get(tableId);

        if (columnBlock == null) {
            ExceptionUtil.throwSqlExecutionException("删除索引失败，找不到表id{}对应的索引块", tableId);
        }

        long startPos = columnBlock.getStartPos();
        long endPos = columnBlock.getStartPos() + TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
        if (endPos >= fileAccessor.getEndPosition()) {
            fileAccessor.truncate(startPos);
        } else {
            int blockIndex = columnBlock.getBlockIndex();
            long oldNextStarPos = endPos;
            while (oldNextStarPos < fileAccessor.getEndPosition()) {
                ByteBuffer readBuffer = fileAccessor.read(oldNextStarPos, TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE);
                TableIndexBlock block = new TableIndexBlock(readBuffer);
                block.setBlockIndex(blockIndex);

                fileAccessor.write(block.getByteBuffer(), startPos);
                startPos += TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
                oldNextStarPos += TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
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
        long endPosition = fileAccessor.getEndPosition();
        if (endPosition >= TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE) {
            long currPos = 0;
            while (currPos < endPosition) {
                ByteBuffer readBuffer = fileAccessor.read(currPos, TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE);
                TableIndexBlock columnBlock = new TableIndexBlock(readBuffer);
                indexBlockList.add(columnBlock);
                currPos += TableIndexBlock.TABLE_COLUMN_BLOCK_SIZE;
            }

            for (TableIndexBlock columnBlock : indexBlockList) {
                indexBlockMap.put(columnBlock.getTableId(), columnBlock);
            }
        }
    }

}
