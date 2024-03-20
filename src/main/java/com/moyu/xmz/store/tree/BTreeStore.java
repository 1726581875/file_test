package com.moyu.xmz.store.tree;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.common.util.FileUtils;
import com.moyu.xmz.store.accessor.FileAccessor;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/6/30
 */
public class BTreeStore {

    public static final int PAGE_START_POS = 4096;

    private FileAccessor fileAccessor;

    private int pageCount;

    private int nextPageIndex;

    private long rootStartPos;

    private long nextRowId;

    public BTreeStore(String fileFullPath) throws IOException {
        FileUtils.createFileIfNotExists(fileFullPath);
        this.fileAccessor = new FileAccessor(fileFullPath);
        long endPosition = fileAccessor.getEndPosition();
        // 前8字节是记录根节点位置
        if(endPosition >= PAGE_START_POS) {
            this.rootStartPos = DataByteUtils.readLong(fileAccessor.read(0, CommonConstant.LONG_LENGTH));
            this.nextRowId = DataByteUtils.readLong(fileAccessor.read(8, CommonConstant.LONG_LENGTH));
            this.pageCount = (int) ((endPosition - PAGE_START_POS) / Page.PAGE_SIZE);

        } else {
            this.rootStartPos = 0L;
            this.pageCount = 0;
            this.nextRowId = 0L;
        }
        this.nextPageIndex = this.pageCount == 0 ? 0 : this.pageCount;
    }




    public Page getRootPage(BTreeMap map){
        return getPage(rootStartPos, map);
    }

    public void updateRootPos(long rootPos) {
        ByteBuffer buffer = ByteBuffer.allocate(CommonConstant.LONG_LENGTH);
        DataByteUtils.writeLong(buffer, rootPos);
        buffer.rewind();
        fileAccessor.write(buffer, 0);
        this.rootStartPos = rootPos;
    }


    public Page getPage(long startPos, BTreeMap map) {
        ByteBuffer byteBuffer = fileAccessor.read(startPos, Page.PAGE_SIZE);
        return Page.readPageByByteBuffer(byteBuffer, map);
    }

    public void savePage(Page page) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        ByteBuffer buffer = page.getByteBuffer();
        byteBuffer.put(buffer);
        byteBuffer.rewind();
        fileAccessor.write(byteBuffer, page.getStartPos());
    }


    public void clear() {
        fileAccessor.truncate(0L);
        this.nextPageIndex = 0;
        this.pageCount = 0;
    }


    public int getNextPageIndex() {
        synchronized (this) {
            int next = this.nextPageIndex;
            nextPageIndex++;
            pageCount++;
            return next;
        }
    }

    public long getNextRowId() {
        synchronized (this) {
            long next = this.nextRowId;
            nextRowId++;
            updateNextRowId();
            return next;
        }

    }

    public void updateNextRowId() {
        ByteBuffer buffer = ByteBuffer.allocate(CommonConstant.LONG_LENGTH);
        DataByteUtils.writeLong(buffer, nextRowId);
        buffer.rewind();
        fileAccessor.write(buffer, 8);
    }



    public int getPageCount() {
        return pageCount;
    }


    public void close(){
        fileAccessor.close();
    }
}
