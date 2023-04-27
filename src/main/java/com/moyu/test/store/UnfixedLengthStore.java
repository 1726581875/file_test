package com.moyu.test.store;

import com.moyu.test.util.DataUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class UnfixedLengthStore {

    private String fileFullPath;

    private FileStore fileStore;

    private int currentPos;

    private long endPos;

    public UnfixedLengthStore(String fileFullPath) throws IOException {
        this.fileFullPath = fileFullPath;
        this.fileStore = new FileStore(fileFullPath);
        this.currentPos = 0;
        this.endPos = this.fileStore.getEndPosition();
    }


    public Chunk getNextChunk() {
        if (currentPos >= endPos) {
            return null;
        }
        try {
            synchronized (this) {
                int chunkLenAttrStartPos = currentPos + 4;
                ByteBuffer chunkLenBuff = fileStore.read(chunkLenAttrStartPos, 4);
                int chunkLen = DataUtils.readInt(chunkLenBuff);
                ByteBuffer byteBuff = fileStore.read(currentPos, chunkLen);
                Chunk chunk = new Chunk(byteBuff);
                currentPos += chunkLen;
                return chunk;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close(){
        if (fileStore != null) {
            fileStore.close();
        }
    }
}
