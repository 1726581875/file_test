package test.store.metadata;

import com.moyu.xmz.common.constant.ColumnTypeConstant;
import com.moyu.xmz.store.accessor.ColumnMetaFileAccessor;
import com.moyu.xmz.store.accessor.DatabaseMetaFileAccessor;
import com.moyu.xmz.store.accessor.TableMetaFileAccessor;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.meta.ColumnMetadata;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.common.util.FileUtil;

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

        FileUtil.deleteOnExists(filePath + ColumnMetaFileAccessor.COLUMN_META_FILE_NAME);
        FileUtil.deleteOnExists(filePath + TableMetaFileAccessor.TABLE_META_FILE_NAME);
        FileUtil.deleteOnExists(filePath + DatabaseMetaFileAccessor.DATABASE_META_FILE_NAME);

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
        DatabaseMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new DatabaseMetaFileAccessor(filePath);
            metadataStore.createDatabase("xmz1");
            metadataStore.getAllData().forEach(System.out::println);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
    }

    private static void testTable(String tableName) {
        TableMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(0, filePath);
            metadataStore.createTable(tableName, null);
            TableMetaFileAccessor finalMetadataStore = metadataStore;
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
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
        TableMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(0, filePath);
            metadataStore.dropTable(tableName);
            TableMetaFileAccessor finalMetadataStore = metadataStore;
            System.out.println("==== drop table ==== ");
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
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


    private static void testShowTables() {
        TableMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new TableMetaFileAccessor(0, filePath);
            TableMetaFileAccessor finalMetadataStore = metadataStore;
            System.out.println("==== drop table ==== ");
            metadataStore.getCurrDbAllTable().forEach(tableMetadata -> {
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
        ColumnMetaFileAccessor metadataStore = null;
        try {
            metadataStore = new ColumnMetaFileAccessor(filePath);
            List<Column> columnDtoList = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Column columnDto = new Column("column_" + i, ColumnTypeConstant.VARCHAR, i, 64);
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
