package test.metadata;

import com.moyu.test.store.metadata.DatabaseMetadataStore;

import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class MetadataStoreTest {

    public static void main(String[] args) throws IOException {

        String filePath = "D:\\mytest\\fileTest\\";
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore(filePath);
            metadataStore.createDatabase("xmz3");
            metadataStore.getAllData().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }


    }



}
