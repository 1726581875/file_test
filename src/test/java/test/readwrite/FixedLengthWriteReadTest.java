package test.readwrite;

import test.readwrite.entity.Chunk;
import com.moyu.xmz.store.accessor.FileAccessor;
import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.common.util.FileUtils;
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
        FileUtils.createFileIfNotExists(filePath);
        FileAccessor fileAccessor = null;

        List<Chunk> chunkList = new ArrayList<>();
        for (int i = 0; i < 1024; i++) {
            Chunk chunk = new Chunk(i * 1024, "Hello World " + i);
            chunkList.add(chunk);
        }
        // 固定存储1024字节
        int chunkLen = 1024;
        try {
            fileAccessor = new FileAccessor(filePath);
            for (int i = 0; i < chunkList.size(); i++) {
                String data = chunkList.get(i).getData();
                ByteBuffer byteBuffer = ByteBuffer.allocate(chunkLen);
                DataByteUtils.writeStringData(byteBuffer, data, data.length());
                byteBuffer.rewind();
                fileAccessor.write(byteBuffer, chunkLen * i);
            }
            // 读文件
            for (int i = 0; i < chunkList.size(); i++) {
                ByteBuffer readBuff = fileAccessor.read(chunkLen * i, chunkLen);
                System.out.println(new String(readBuff.array()));
            }

            System.out.println("=== 取单个值 ===");
            int index = 520;
            ByteBuffer readBuff = fileAccessor.read(chunkLen * index, chunkLen);
            System.out.println("取下标520=" + new String(readBuff.array()));


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileAccessor != null) {
                fileAccessor.close();
            }
        }
    }


}
