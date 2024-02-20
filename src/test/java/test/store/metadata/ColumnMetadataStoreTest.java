package test.store.metadata;

import com.moyu.xmz.common.util.FileUtils;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class ColumnMetadataStoreTest {


    private static String filePath = "D:\\mytest\\fileTest\\";


    public static void main(String[] args) {
        FileUtils.deleteOnExists(filePath + ColumnMetaAccessor.COLUMN_META_FILE_NAME);
    }


}
