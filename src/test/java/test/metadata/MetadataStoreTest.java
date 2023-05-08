package test.metadata;

import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;

import java.io.IOException;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class MetadataStoreTest {

    private static String filePath = "D:\\mytest\\fileTest\\";

    public static void main(String[] args) throws IOException {
        testDatabase();
        testTable();
    }


    private static void testDatabase() {
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore(filePath);
            //metadataStore.createDatabase("xmz3");
            metadataStore.getAllData().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }

    private static void testTable() {
        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(0, filePath);
            metadataStore.createTable("xmz_table2");
            metadataStore.getAllTable().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }




}
