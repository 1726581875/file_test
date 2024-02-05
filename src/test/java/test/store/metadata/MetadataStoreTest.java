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

        testDatabase();

        testTable("xmz01");
        testTable("xmz02");
        testTable("xmz03");
        testTable("xmz04");
        testTable("xmz05");
        testTable("xmz06");

        testDropTable("xmz05");


        testShowTables();
        //testColumn();


    }


    private static void testDatabase() {
        DatabaseMetaAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaAccessor(filePath);
            metadataStore.createDatabase("xmz1");
            metadataStore.getAllData().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }

    private static void testTable(String tableName) {
        TableMetaAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaAccessor(0, filePath);
            metadataStore.createTable(tableName, null);
            TableMetaAccessor finalMetadataStore = metadataStore;
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
                System.out.println("==== table ==== ");
                System.out.println(tableMetadata);
                List<ColumnMeta> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
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
        TableMetaAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaAccessor(0, filePath);
            metadataStore.dropTable(tableName);
            TableMetaAccessor finalMetadataStore = metadataStore;
            System.out.println("==== drop table ==== ");
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
                System.out.println(tableMetadata);
                List<ColumnMeta> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
                columnList.forEach(System.out::println);
            });
            System.out.println("==== drop table end==== ");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }


    private static void testShowTables() {
        TableMetaAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaAccessor(0, filePath);
            TableMetaAccessor finalMetadataStore = metadataStore;
            System.out.println("==== drop table ==== ");
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
                System.out.println(tableMetadata);
                List<ColumnMeta> columnList = finalMetadataStore.getColumnList(tableMetadata.getTableId());
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
        ColumnMetaAccessor metadataStore = null;
        try {
            metadataStore = new ColumnMetaAccessor(filePath);
            List<Column> columnDtoList = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Column columnDto = new Column("column_" + i, DbTypeConstant.VARCHAR, i, 64);
                columnDtoList.add(columnDto);
            }
            metadataStore.createColumnBlock(0, columnDtoList);
            Map<Integer, TableColumnBlock> columnMap = metadataStore.getColumnMap();
            columnMap.forEach((k,v) -> {
                System.out.println("=======  tableId="+ k +" ========");
                v.getColumnMetaList().forEach(System.out::println);
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }




}
