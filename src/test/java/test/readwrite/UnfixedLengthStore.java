package test.readwrite;

import com.moyu.xmz.store.accessor.FileAccessor;
import com.moyu.xmz.common.util.DataByteUtils;
import test.readwrite.entity.Chunk;
import test.readwrite.entity.FileHeader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class UnfixedLengthStore {

    private String fileFullPath;

    private FileAccessor fileAccessor;

    private long currentPos;

    private FileHeader fileHeader;

    public UnfixedLengthStore(String fileFullPath) throws IOException {
        this.fileFullPath = fileFullPath;
        this.fileAccessor = new FileAccessor(fileFullPath);
        ByteBuffer headerBuffer = fileAccessor.read(0, FileHeader.HEADER_LENGTH);
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
                ByteBuffer chunkLenBuff = fileAccessor.read(chunkLenAttrStartPos, 4);
                int chunkLen = DataByteUtils.readInt(chunkLenBuff);
                ByteBuffer byteBuff = fileAccessor.read(currentPos, chunkLen);
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
            synchronized (fileFullPath.intern()) {
                // write chunk
                ByteBuffer byteBuffer = chunk.getByteBuff();
                fileAccessor.write(byteBuffer, fileHeader.getFileEndPos());

                // write header
                fileHeader.setFileEndPos(fileHeader.getFileEndPos() + chunk.getChunkLen());
                fileHeader.setTotalChunkNum(fileHeader.getTotalChunkNum() + 1);
                ByteBuffer headerBuff = fileHeader.getByteBuff();
                fileAccessor.write(headerBuff, 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void createAndInitFile(String fileFullPath) throws IOException {
        File file = new File(fileFullPath);
        if (!file.exists()) {
            file.createNewFile();
        } else {
            throw new RuntimeException("文件已存在");
        }

        FileAccessor fileAccessor = new FileAccessor(fileFullPath);
        try {
            // init header
            FileHeader fileHeader = new FileHeader(FileHeader.HEADER_LENGTH,
                    0, 0, FileHeader.HEADER_LENGTH);
            ByteBuffer headerBuff = fileHeader.getByteBuff();
            fileAccessor.write(headerBuff, 0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileAccessor.close();
        }
    }



    public void close() {
        if (fileAccessor != null) {
            fileAccessor.close();
        }
    }
}
