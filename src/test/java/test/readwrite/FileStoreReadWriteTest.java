package test.readwrite;

import com.moyu.test.store.FileStore;
import com.moyu.test.util.FileUtil;

import java.nio.ByteBuffer;


/**
 * @author xiaomingzhang
 * @date 2023/4/18
 */
public class FileStoreReadWriteTest {

    private static String filePath = "D:\\mytest\\fileTest\\moyu.xmz";

    public static void main(String[] args) {
        FileUtil.createFileIfNotExists(filePath);
        FileStore fileStore = null;
        try {
            fileStore = new FileStore(filePath);
            // 写文件
            String str = "Hello World !";
            ByteBuffer byteBuffer = ByteBuffer.wrap(str.getBytes());
            fileStore.write(byteBuffer, 0);

            // 读文件
            ByteBuffer readBuff = fileStore.read(0, str.length());
            System.out.println(new String(readBuff.array(), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileStore != null) {
                fileStore.close();
            }
        }
    }


}
