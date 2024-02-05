package test.store.metadata;

import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.store.common.block.TableIndexBlock;

import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class IndexMetadataStoreTest {

    public static void main(String[] args) {
        IndexMetaAccessor metadataStore = null;
        try {
            metadataStore = new IndexMetaAccessor(0);
            IndexMeta index = new IndexMeta(0L, 1, "aaa", "bbb", (byte) 0);
            //metadataStore.saveIndexMetadata(2, index);
            //metadataStore.dropIndexMetadata(1, "indexName");
            metadataStore.dropIndexBlock(2);
            Map<Integer, TableIndexBlock> columnMap = metadataStore.getIndexMap();
            columnMap.forEach((k, v) -> {
                System.out.println("=======  tableId=" + k + " ========");
                v.getIndexMetaList().forEach(System.out::println);

                System.out.println(v);
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }
}
