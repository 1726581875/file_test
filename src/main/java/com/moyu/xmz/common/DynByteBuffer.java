package com.moyu.xmz.common;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.common.util.DataByteUtils;

import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2024/1/29
 * 一个动态扩容的ByteBuffer
 */
public class DynByteBuffer {

    private ByteBuffer buffer;

    private static final int DEFAULT_CAPACITY = 1024;

    private static final int MAX_SIZE = Integer.MAX_VALUE - 8;

    public DynByteBuffer() {
        this.buffer = ByteBuffer.allocate(DEFAULT_CAPACITY);
    }

    public DynByteBuffer(int capacity) {
        this.buffer = ByteBuffer.allocate(capacity);
    }

    public void put(byte value) {
        ensureCapacityInternal(getMinCapacity(1));
        buffer.put(value);
    }

    public void put(byte[] value) {
        ensureCapacityInternal(getMinCapacity(value.length));
        buffer.put(value);
    }

    public void put(ByteBuffer byteBuffer) {
        ensureCapacityInternal(getMinCapacity(byteBuffer.remaining()));
        buffer.put(byteBuffer);
    }

    public void putInt(int value) {
        ensureCapacityInternal(getMinCapacity(4));
        DataByteUtils.writeInt(buffer, value);
    }

    public void putInt(int index, int value){
        byte[] bytes = DataByteUtils.intToBytes(value);
        buffer.put(index, bytes[0]);
        buffer.put(index + 1, bytes[1]);
        buffer.put(index + 2, bytes[2]);
        buffer.put(index + 3, bytes[3]);
    }

    public void putLong(long v) {
        ensureCapacityInternal(getMinCapacity(8));
        DataByteUtils.writeLong(buffer, v);
    }

    public void putDouble(double v) {
        ensureCapacityInternal(getMinCapacity(8));
        buffer.putDouble(v);
    }

    public void putString(String v) {
        ensureCapacityInternal(getMinCapacity(v.length() * 3));
        DataByteUtils.writeStringData(buffer, v, v.length());
    }

    private void ensureCapacityInternal(int minCapacity) {
        int oldCapacity = buffer.capacity();
        if(minCapacity > oldCapacity) {
            grow(minCapacity);
        }
    }

    /**
     * 获取所需要的最小容量
     * @param len
     * @return
     */
    private int getMinCapacity(int len) {
        int position = buffer.position();
        return Math.max(position + len, buffer.capacity());
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
        //System.out.println("DynamicByteBuffer扩容,旧容量=" + oldCapacity + ",新=" + newCapacity);
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

    public byte[] flipAndGetBytes() {
        this.buffer.flip();
        byte[] result = new byte[this.buffer.limit()];
        this.buffer.get(result);
        return result;
    }

    public ByteBuffer flipAndGetBuffer() {
        buffer.flip();
        return buffer;
    }

}
