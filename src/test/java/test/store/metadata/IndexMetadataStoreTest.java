package test.store.metadata;

import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.obj.IndexMetadata;
import com.moyu.test.store.metadata.obj.TableIndexBlock;

import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/30
 */
public class IndexMetadataStoreTest {

    public static void main(String[] args) {
        IndexMetadataStore metadataStore = null;
        try {
            metadataStore = new IndexMetadataStore();
            IndexMetadata index = new IndexMetadata(0L, 1, "aaa", "bbb", (byte) 0);
           // metadataStore.saveIndexMetadata(2, index);
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
