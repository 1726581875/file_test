package com.moyu.xmz.store.buffer;

import com.moyu.xmz.store.accessor.DataChunkFileAccessor;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.common.util.PathUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class BufferPoolManager {

    /**
     * key: databaseId.tableName
     * value: Map<块地址开始位置, 数据块>
     */
    private static final Map<String, Map<Long, BufferDataChunk>> bufferPool = new HashMap<>();


    /**
     * TODO 应当限制缓存大小
     * @param databaseId
     * @param tableName
     * @param pos
     * @return
     */
    public static BufferDataChunk getDataChunk(Integer databaseId, String tableName, Long pos) {
        String key = getKey(databaseId, tableName);
        if (bufferPool.containsKey(key)) {
            Map<Long, BufferDataChunk> dataChunkMap = bufferPool.get(key);
            if (dataChunkMap.containsKey(pos)) {
                return dataChunkMap.get(pos);
            } else {
                BufferDataChunk bufferDataChunk = getDiskDataChunk(databaseId, tableName, pos);
                dataChunkMap.put(pos, bufferDataChunk);
                return bufferDataChunk;
            }
        } else {
            Map<Long, BufferDataChunk> dataChunkMap = new HashMap<>();
            BufferDataChunk bufferDataChunk = getDiskDataChunk(databaseId, tableName, pos);
            dataChunkMap.put(pos, bufferDataChunk);
            bufferPool.put(key, dataChunkMap);
            return bufferDataChunk;
        }
    }

    private static BufferDataChunk getDiskDataChunk(Integer databaseId, String tableName, Long pos) {
        DataChunkFileAccessor dataChunkFileAccessor = null;
        try {
            dataChunkFileAccessor = new DataChunkFileAccessor(PathUtil.getDataFilePath(databaseId, tableName));
            DataChunk chunk = dataChunkFileAccessor.getChunkByPos(pos);
            if (chunk == null) {
                throw new DbException("获取数据块发生异常,块不存在pos:" + pos);
            }
            BufferDataChunk bufferDataChunk = new BufferDataChunk(databaseId, tableName, BufferDataChunk.NOT_MODIFIED, chunk);
            return bufferDataChunk;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DbException("获取数据块发生异常");
        } finally {
            dataChunkFileAccessor.close();
        }
    }


    private static String getKey(Integer databaseId, String tableName){
        return databaseId + "." + tableName;
    }

}
