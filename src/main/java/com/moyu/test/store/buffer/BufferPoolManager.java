package com.moyu.test.store.buffer;

import java.util.HashMap;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class BufferPoolManager {

    // 缓冲区池
    private HashMap<Integer, byte[]> bufferPool;
    // 缓冲区大小
    private int bufferSize;
    // 最大缓冲区数量
    private int maxBufferCount;

    public BufferPoolManager(int bufferSize, int maxBufferCount) {
        this.bufferSize = bufferSize;
        this.maxBufferCount = maxBufferCount;
        bufferPool = new HashMap<>();
    }

    // 从缓冲区池中获取指定页号的缓冲区
    public byte[] getBuffer(int pageNum) {
        if (bufferPool.containsKey(pageNum)) {
            return bufferPool.get(pageNum);
        } else {
            // 如果缓冲区池中没有该页号的缓冲区，则需要创建一个新的缓冲区
            if (bufferPool.size() >= maxBufferCount) {
                // 如果缓冲区池已满，则需要淘汰一个缓冲区
                evictBuffer();
            }
            byte[] buffer = new byte[bufferSize];
            bufferPool.put(pageNum, buffer);
            return buffer;
        }
    }

    // 将指定页号的缓冲区从缓冲区池中移除
    public void removeBuffer(int pageNum) {
        if (bufferPool.containsKey(pageNum)) {
            bufferPool.remove(pageNum);
        }
    }

    // 淘汰一个缓冲区
    private void evictBuffer() {
        int evictPageNum = bufferPool.keySet().iterator().next();
        removeBuffer(evictPageNum);
    }


}
