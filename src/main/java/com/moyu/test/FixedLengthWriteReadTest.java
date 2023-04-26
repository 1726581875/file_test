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
public class FixedLengthWriteReadTest {


    public static void main(String[] args) {
        String filePath = "D:\\mytest\\fileTest\\fixLen.xmz";
        FileUtil.createFileIfNotExists(filePath);
        FileStore fileStore = null;

        List<Chunk> chunkList = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            Chunk chunk = new Chunk(i * 1024, "Hello World " + i);
            chunkList.add(chunk);
        }
        // 固定存储1024字节
        int chunkLen = 1024;
        try {
            fileStore = new FileStore(filePath);
            for (int i = 0; i < chunkList.size(); i++) {
                String data = chunkList.get(i).getData();
                ByteBuffer byteBuffer = ByteBuffer.allocate(chunkLen);
                DataUtils.writeStringData(byteBuffer, data, data.length());
                byteBuffer.rewind();
                fileStore.write(byteBuffer, chunkLen * i);
            }
            // 读文件
            for (int i = 0; i < chunkList.size(); i++) {
                ByteBuffer readBuff = fileStore.read(chunkLen * i, chunkLen);
                System.out.println(new String(readBuff.array()));
            }

            System.out.println("=== 取单个值 ===");
            int index = 520;
            ByteBuffer readBuff = fileStore.read(chunkLen * index, chunkLen);
            System.out.println("取下标520=" + new String(readBuff.array()));


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }


}
