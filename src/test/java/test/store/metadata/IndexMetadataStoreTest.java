package test.store.metadata;

import com.moyu.xmz.store.accessor.IndexMetaFileAccessor;
import com.moyu.xmz.store.common.meta.IndexMetadata;
import com.moyu.xmz.store.common.block.TableIndexBlock;

import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class IndexMetadataStoreTest {

    public static void main(String[] args) {
        IndexMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new IndexMetaFileAccessor(0);
            IndexMetadata index = new IndexMetadata(0L, 1, "aaa", "bbb", (byte) 0);
            //metadataStore.saveIndexMetadata(2, index);
            //metadataStore.dropIndexMetadata(1, "indexName");
            metadataStore.dropIndexBlock(2);
            Map<Integer, TableIndexBlock> columnMap = metadataStore.getIndexMap();
            columnMap.forEach((k, v) -> {
                System.out.println("=======  tableId=" + k + " ========");
                v.getIndexMetadataList().forEach(System.out::println);

                System.out.println(v);
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }
}
