package com.moyu.test.store.data.tree;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.FileStore;
import com.moyu.test.util.DataUtils;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/5/25
 */
public class BpTreeStore {

    private static final String defaultPath = PathUtil.getBaseDirPath();

    private static final String fileName = "tree.d";

    private FileStore fileStore;

    private int pageCount;

    private int nextPageIndex;

    private long rootStartPos;


    public BpTreeStore() throws IOException {
        this(defaultPath + File.separator + fileName);
    }

    public BpTreeStore(String fileFullPath) throws IOException {
        FileUtil.createFileIfNotExists(fileFullPath);
        this.fileStore = new FileStore(fileFullPath);
        long endPosition = fileStore.getEndPosition();
        // 前8字节是记录根节点位置
        if(endPosition >= JavaTypeConstant.LONG_LENGTH) {
            ByteBuffer byteBuffer = fileStore.read(0, JavaTypeConstant.LONG_LENGTH);
            this.rootStartPos = DataUtils.readLong(byteBuffer);
            this.pageCount = (int) ((endPosition - 8) / Page.PAGE_SIZE);
        } else {
            this.rootStartPos = 0L;
            this.pageCount = 0;
        }
        this.nextPageIndex = this.pageCount == 0 ? 0 : this.pageCount;
    }




    public Page getRootPage(BpTreeMap map){
        return getPage(rootStartPos, map);
    }

    public void updateRootPos(long rootPos) {
        ByteBuffer buffer = ByteBuffer.allocate(JavaTypeConstant.LONG_LENGTH);
        DataUtils.writeLong(buffer, rootPos);
        buffer.rewind();
        fileStore.write(buffer, 0);
        this.rootStartPos = rootPos;
    }


    public Page getPage(long startPos, BpTreeMap map) {
        ByteBuffer byteBuffer = fileStore.read(startPos, Page.PAGE_SIZE);
        return new Page(byteBuffer, map);
    }

    public void savePage(Page page) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
        ByteBuffer buffer = page.getByteBuffer();
        byteBuffer.put(buffer);
        byteBuffer.rewind();
        fileStore.write(byteBuffer, page.getStartPos());
    }


    public void clear() {
        fileStore.truncate(0L);
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

    public int getPageCount() {
        return pageCount;
    }


    public void close(){
        fileStore.close();
    }
}
