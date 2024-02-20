package test.store.metadata;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.store.accessor.ColumnMetaAccessor;
import com.moyu.xmz.store.accessor.DatabaseMetaAccessor;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMeta;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.common.util.FileUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class MetadataStoreTest {

    private static String filePath = "D:\\mytest\\fileTest\\";

    public static void main(String[] args) throws IOException, InterruptedException {

        FileUtils.deleteOnExists(filePath + ColumnMetaAccessor.COLUMN_META_FILE_NAME);
        FileUtils.deleteOnExists(filePath + TableMetaAccessor.TABLE_META_FILE_NAME);
        FileUtils.deleteOnExists(filePath + DatabaseMetaAccessor.DATABASE_META_FILE_NAME);
    }





}
