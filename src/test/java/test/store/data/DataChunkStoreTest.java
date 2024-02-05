package test.store.data;

import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.store.common.block.DataChunk;
import com.moyu.xmz.store.accessor.DataChunkAccessor;
import com.moyu.xmz.store.common.meta.RowMeta;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.common.util.FileUtils;

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

        FileUtils.deleteOnExists(filePath);

        DataChunkAccessor dataChunkAccessor = null;
        try {
            dataChunkAccessor = new DataChunkAccessor(filePath);
            // create a chunk
            dataChunkAccessor.createChunk();


            List<Column> columnList = new ArrayList<>();
            List<Column> columnList2 = new ArrayList<>();

            // 字段信息有值，用于写
            Column column0 = new Column("a", DbTypeConstant.INT_4, 0, 4);
            column0.setValue(99);
            Column column1 = new Column("b", DbTypeConstant.VARCHAR, 1, 10);
            column1.setValue("520");

            columnList.add(column0);
            columnList.add(column1);


            // 字段信息没有值，用于查询
            Column column3 = new Column("a", DbTypeConstant.INT_4, 0, 4);
            Column column4 = new Column("b", DbTypeConstant.VARCHAR, 1, 10);
            columnList2.add(column3);
            columnList2.add(column4);


            // write data
            for (int i = 0; i < 1; i++) {
                try {
                    byte[] bytes = RowMeta.toRowByteData(columnList);
                    dataChunkAccessor.storeRow(bytes,false);
                    System.out.println("currNum=" + i);
                } catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }

            // print
            DataChunk chunk = dataChunkAccessor.getChunk(0);
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
            if (dataChunkAccessor != null) {
                dataChunkAccessor.close();
            }
        }

    }

    private static void testWriteRow() {
        String filePath = "D:\\mytest\\fileTest\\test.d";

        FileUtils.deleteOnExists(filePath);

        DataChunkAccessor dataChunkAccessor = null;
        try {
            dataChunkAccessor = new DataChunkAccessor(filePath);
            // create a chunk
            dataChunkAccessor.createChunk();

            // write data
            for (int i = 0; i < 1024; i++) {
                try {
                    dataChunkAccessor.storeRow("hello world!hello world!啊啊啊啊".getBytes(), false);
                    System.out.println("currNum=" + i);
                } catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }

            // print
            DataChunk chunk = dataChunkAccessor.getChunk(0);
            System.out.println(chunk);
            chunk.getDataRowList().forEach(row -> {
                System.out.println(row);
                System.out.println(new String(row.getRow()));
            });
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dataChunkAccessor != null) {
                dataChunkAccessor.close();
            }
        }
    }


}
