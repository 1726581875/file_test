package com.moyu.test;

import com.moyu.test.store.Chunk;
import com.moyu.test.store.FileStore;
import com.moyu.test.util.DataUtils;
import com.moyu.test.util.FileUtil;

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
        FileStore fileStore = null;

        List<Chunk> chunkList = new ArrayList<>();
        int chunkStartPop = 0;
        for (int i = 0; i < 1024; i++) {
            Chunk chunk = new Chunk(chunkStartPop, "Hello World " + i);
            chunkList.add(chunk);
            chunkStartPop += chunk.getChunkLen();
        }

        try {
            fileStore = new FileStore(filePath);

            // 写文件
            for (int i = 0; i < chunkList.size(); i++) {
                Chunk chunk = chunkList.get(i);
                ByteBuffer byteBuffer = chunk.getByteBuff();
                byteBuffer.rewind();
                fileStore.write(byteBuffer, chunk.getChunkStartPos());
            }

            // 读文件
            int startPos = 0;
            for (int i = 0; i < chunkList.size(); i++) {
                // 读取块长度
                int chunkLenAttrStartPos = startPos;
                ByteBuffer chunkLenBuff = fileStore.read(chunkLenAttrStartPos, 4);
                int chunkLen = DataUtils.readInt(chunkLenBuff);


                ByteBuffer byteBuff = fileStore.read(startPos, chunkLen);

                Chunk chunk = new Chunk(byteBuff);
                System.out.println(chunk);

                startPos += chunkLen;
            }

            System.out.println("=== 取单个值 ===");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }


}
