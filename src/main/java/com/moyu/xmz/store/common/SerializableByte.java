package com.moyu.xmz.store.common;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/25
 */
public interface SerializableByte {

    /**
     * 获取字节列表
     * @return
     */
    ByteBuffer getByteBuffer();

}
