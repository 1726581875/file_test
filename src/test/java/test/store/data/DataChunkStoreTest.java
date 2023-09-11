package test.store.data;

import com.moyu.test.constant.ColumnTypeConstant;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.FileUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/12
 */
public class DataChunkStoreTest {


    public static void main(String[] args) {
        testWriteRow2();
    }


    private static void testWriteRow2() {

        String filePath = "D:\\mytest\\fileTest\\test2.d";

        FileUtil.deleteOnExists(filePath);

        DataChunkStore dataChunkStore = null;
        try {
            dataChunkStore = new DataChunkStore(filePath);
            // create a chunk
            dataChunkStore.createChunk();


            List<Column> columnList = new ArrayList<>();
            List<Column> columnList2 = new ArrayList<>();

            // 字段信息有值，用于写
            Column column0 = new Column("a", ColumnTypeConstant.INT_4, 0, 4);
            column0.setValue(99);
            Column column1 = new Column("b", ColumnTypeConstant.VARCHAR, 1, 10);
            column1.setValue("520");

            columnList.add(column0);
            columnList.add(column1);


            // 字段信息没有值，用于查询
            Column column3 = new Column("a", ColumnTypeConstant.INT_4, 0, 4);
            Column column4 = new Column("b", ColumnTypeConstant.VARCHAR, 1, 10);
            columnList2.add(column3);
            columnList2.add(column4);


            // write data
            for (int i = 0; i < 1; i++) {
                try {
                    byte[] bytes = RowData.toRowByteData(columnList);
                    dataChunkStore.storeRow(bytes,false);
                    System.out.println("currNum=" + i);
                } catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }

            // print
            DataChunk chunk = dataChunkStore.getChunk(0);
            System.out.println(chunk);
            chunk.getDataRowList().forEach(row -> {
                System.out.println(row);

                List<Column> columnList1 = row.getColumnList(columnList2);
                columnList1.forEach(System.out::println);
            });
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dataChunkStore != null) {
                dataChunkStore.close();
            }
        }

    }

    private static void testWriteRow() {
        String filePath = "D:\\mytest\\fileTest\\test.d";

        FileUtil.deleteOnExists(filePath);

        DataChunkStore dataChunkStore = null;
        try {
            dataChunkStore = new DataChunkStore(filePath);
            // create a chunk
            dataChunkStore.createChunk();

            // write data
            for (int i = 0; i < 1024; i++) {
                try {
                    dataChunkStore.storeRow("hello world!hello world!啊啊啊啊".getBytes(), false);
                    System.out.println("currNum=" + i);
                } catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }

            // print
            DataChunk chunk = dataChunkStore.getChunk(0);
            System.out.println(chunk);
            chunk.getDataRowList().forEach(row -> {
                System.out.println(row);
                System.out.println(new String(row.getRow()));
            });
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dataChunkStore != null) {
                dataChunkStore.close();
            }
        }
    }


}
