package test.readwrite;

import test.readwrite.entity.Chunk;
import test.readwrite.entity.FileHeader;
import com.moyu.xmz.store.accessor.FileAccessor;
import com.moyu.xmz.common.util.DataUtils;
import com.moyu.xmz.common.util.FileUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/4/26
 */
public class UnfixedLengthWriteReadTest {

    public static void main(String[] args) {
        String filePath = "D:\\mytest\\fileTest\\unfixLen.xmz";
        FileUtil.createFileIfNotExists(filePath);
        FileAccessor fileAccessor = null;

        // header
        FileHeader fileHeader = new FileHeader(FileHeader.HEADER_LENGTH ,
                0, 0, FileHeader.HEADER_LENGTH);
        ByteBuffer headerBuff = fileHeader.getByteBuff();


        // chunk list
        List<Chunk> chunkList = new ArrayList<>();
        long chunkStartPop = fileHeader.getFirstChunkStartPos();
        for (int i = 0; i < 1024; i++) {
            Chunk chunk = new Chunk(chunkStartPop, "Hello World " + i);
            chunkList.add(chunk);
            chunkStartPop += chunk.getChunkLen();
        }

        try {
            fileAccessor = new FileAccessor(filePath);

            // write header
            headerBuff.rewind();
            fileAccessor.write(headerBuff, 0);

            // 写文件
            for (int i = 0; i < chunkList.size(); i++) {
                Chunk chunk = chunkList.get(i);
                ByteBuffer byteBuffer = chunk.getByteBuff();
                byteBuffer.rewind();
                fileAccessor.write(byteBuffer, chunk.getChunkStartPos());
            }

            // end write header
            fileHeader.setTotalChunkNum(1024);
            fileHeader.setFileEndPos(chunkStartPop);
            ByteBuffer endFileBuff = fileHeader.getByteBuff();
            endFileBuff.rewind();
            fileAccessor.write(endFileBuff, 0);

            // 读文件
            long startPos = fileHeader.getFirstChunkStartPos();
            for (int i = 0; i < chunkList.size(); i++) {
                // 读取块长度
                long chunkLenAttrStartPos = startPos;
                ByteBuffer chunkLenBuff = fileAccessor.read(chunkLenAttrStartPos, 4);
                int chunkLen = DataUtils.readInt(chunkLenBuff);


                ByteBuffer byteBuff = fileAccessor.read(startPos, chunkLen);

                Chunk chunk = new Chunk(byteBuff);
                System.out.println(chunk);

                startPos += chunkLen;
            }

            System.out.println("=== 取单个值 ===");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileAccessor != null) {
                fileAccessor.close();
            }
        }
    }


}
