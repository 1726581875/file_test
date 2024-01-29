package com.moyu.xmz.common;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.common.util.DataUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2024/1/29
 * 一个动态扩容的ByteBuffer
 */
public class DynamicByteBuffer {

    private ByteBuffer buffer;

    private static final int DEFAULT_CAPACITY = 1024;

    private static final int MAX_SIZE = Integer.MAX_VALUE - 8;

    public DynamicByteBuffer() {
        this.buffer = ByteBuffer.allocate(DEFAULT_CAPACITY);
    }

    public DynamicByteBuffer(int capacity) {
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public void put(byte value) {
        ensureCapacityInternal(buffer.capacity() + 1);
        buffer.put(value);
    }

    public void putInt(int value) {
        ensureCapacityInternal(buffer.capacity() + 4);
        DataUtils.writeInt(buffer, value);
    }

    public void putInt(int index, int value){
        buffer.putInt(index, value);
    }

    public void putLong(long v) {
        ensureCapacityInternal(buffer.capacity() + 8);
        DataUtils.writeLong(buffer, v);
    }

    public void putString(String v) {
        ensureCapacityInternal(buffer.capacity() + v.length() * 3);
        DataUtils.writeStringData(buffer, v, v.length());
    }

    private void ensureCapacityInternal(int minCapacity) {
        int oldCapacity = buffer.capacity();
        if(minCapacity > oldCapacity) {
            grow(minCapacity);
        }
    }

    private void grow(int minCapacity) {
        int oldCapacity = buffer.capacity();
        // 每次扩容是原来的1.5倍
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        // 扩容后还是少于必须的容量，直接拿minCapacity
        if (newCapacity < minCapacity) {
            newCapacity = minCapacity;
        }
        if (newCapacity > MAX_SIZE) {
            throw new DbException("DynamicByteBuffer扩容异常,超出最大限制");
        }
        ByteBuffer newByteBuffer = ByteBuffer.allocate(newCapacity);
        buffer.flip();
        newByteBuffer.put(buffer);
        this.buffer = newByteBuffer;
    }

    public int position() {
        return buffer.position();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public ByteBuffer flipAndGetBuffer() {
        buffer.flip();
        return buffer;
    }

}
