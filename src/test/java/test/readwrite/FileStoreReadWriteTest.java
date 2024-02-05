package test.readwrite;

import com.moyu.xmz.store.accessor.FileAccessor;
import com.moyu.xmz.common.util.FileUtils;

import java.nio.ByteBuffer;


/**
 * @author xiaomingzhang
 * @date 2023/4/18
 */
public class FileStoreReadWriteTest {

    private static String filePath = "D:\\mytest\\fileTest\\moyu.xmz";

    public static void main(String[] args) {
        FileUtils.createFileIfNotExists(filePath);
        FileAccessor fileAccessor = null;
        try {
            fileAccessor = new FileAccessor(filePath);
            // 写文件
            String str = "Hello World !";
            ByteBuffer byteBuffer = ByteBuffer.wrap(str.getBytes());
            fileAccessor.write(byteBuffer, 0);

            // 读文件
            ByteBuffer readBuff = fileAccessor.read(0, str.length());
            System.out.println(new String(readBuff.array(), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileAccessor != null) {
                fileAccessor.close();
            }
        }
    }


}
