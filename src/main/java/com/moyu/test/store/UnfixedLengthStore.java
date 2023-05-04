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

    private long currentPos;

    private FileHeader fileHeader;

    public UnfixedLengthStore(String fileFullPath) throws IOException {
        this.fileFullPath = fileFullPath;
        this.fileStore = new FileStore(fileFullPath);
        ByteBuffer headerBuffer = fileStore.read(0, FileHeader.HEADER_LENGTH);
        this.fileHeader = new FileHeader(headerBuffer);
        this.currentPos = this.fileHeader.getFirstChunkStartPos();
    }


    public Chunk getNextChunk() {
        if (currentPos >= fileHeader.getFileEndPos()) {
            return null;
        }
        try {
            synchronized (this) {
                long chunkLenAttrStartPos = currentPos;
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

    public void writeData(String data) {
        try {
            Chunk chunk = new Chunk(fileHeader.getFileEndPos(), data);
            synchronized (fileFullPath) {
                // write chunk
                ByteBuffer byteBuffer = chunk.getByteBuff();
                fileStore.write(byteBuffer, fileHeader.getFileEndPos());

                // write header
                fileHeader.setFileEndPos(fileHeader.getFileEndPos() + chunk.getChunkLen());
                fileHeader.setTotalChunkNum(fileHeader.getTotalChunkNum() + 1);
                ByteBuffer headerBuff = fileHeader.getByteBuff();
                fileStore.write(headerBuff, 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }
}
