package test.store.metadata;

import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.util.FileUtil;

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

        FileUtil.deleteOnExists(filePath + ColumnMetadataStore.COLUMN_META_FILE_NAME);
        FileUtil.deleteOnExists(filePath + TableMetadataStore.TABLE_META_FILE_NAME);
        FileUtil.deleteOnExists(filePath + DatabaseMetadataStore.DATABASE_META_FILE_NAME);

        testDatabase();

        testTable("xmz01");
        testTable("xmz02");
        testTable("xmz03");

        testDropTable("xmz01");

        testColumn();


    }


    private static void testDatabase() {
        DatabaseMetadataStore metadataStore = null;
        try {
            metadataStore = new DatabaseMetadataStore(filePath);
            metadataStore.createDatabase("xmz1");
            metadataStore.getAllData().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }

    private static void testTable(String tableName) {
        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(0, filePath);
            metadataStore.createTable(tableName);
            TableMetadataStore finalMetadataStore = metadataStore;
            metadataStore.getAllTable().forEach(tableMetadata -> {
                System.out.println("==== table ==== ");
                System.out.println(tableMetadata);
                List<ColumnMetadata> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
                columnList.forEach(System.out::println);
                System.out.println("==== table ==== ");
            });

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }


    private static void testDropTable(String tableName) {
        TableMetadataStore metadataStore = null;
        try {
            metadataStore = new TableMetadataStore(0, filePath);
            metadataStore.dropTable(tableName);
            TableMetadataStore finalMetadataStore = metadataStore;
            System.out.println("==== drop table ==== ");
            metadataStore.getAllTable().forEach(tableMetadata -> {
                System.out.println(tableMetadata);
                List<ColumnMetadata> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
                columnList.forEach(System.out::println);
            });
            System.out.println("==== drop table end==== ");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }

    private static void testColumn(){
        ColumnMetadataStore metadataStore = null;
        try {
            metadataStore = new ColumnMetadataStore(filePath);
            List<Column> columnDtoList = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Column columnDto = new Column("column_" + i, DbColumnTypeConstant.VARCHAR, i, 64);
                columnDtoList.add(columnDto);
            }
            metadataStore.createColumnBlock(0, columnDtoList);
            Map<Integer, TableColumnBlock> columnMap = metadataStore.getColumnMap();
            columnMap.forEach((k,v) -> {
                System.out.println("=======  tableId="+ k +" ========");
                v.getColumnMetadataList().forEach(System.out::println);
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }




}
