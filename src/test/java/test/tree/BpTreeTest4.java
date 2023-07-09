package test.tree;

import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.store.data2.BTreeMap;
import com.moyu.test.store.data2.BTreeStore;
import com.moyu.test.store.data2.type.RowValue;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.type.dbtype.IntColumnType;
import com.moyu.test.store.type.obj.RowDataType;
import com.moyu.test.util.FileUtil;
import com.moyu.test.util.PathUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/25
 */
public class BpTreeTest4 {

    private static final String testPath = PathUtil.getBaseDirPath() + File.separator + "b3.data";
    /**
     * 写入磁盘测试
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        FileUtil.deleteOnExists(testPath);

        BTreeStore bpTreeStore = new BTreeStore(testPath);
        try {
            BTreeMap<Integer, RowValue> bTreeMap = new BTreeMap<>(new IntColumnType(), new RowDataType(), bpTreeStore, true);
            bTreeMap.initRootNode();

            int count = 10000;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                List<Column> columnList = new ArrayList<>();
                Column c0 = new Column("id",  ColumnTypeEnum.INT_4.getColumnType(), 0, 4);
                Column c1 = new Column("name_1",  ColumnTypeEnum.VARCHAR.getColumnType(), 1, 36);
                Column c2 = new Column("age",  ColumnTypeEnum.INT_4.getColumnType(), 2, 4);

                c0.setValue(i);
                c1.setValue("250_" + i);
                c2.setValue(i);

                columnList.add(c0);
                columnList.add(c1);
                columnList.add(c2);

                Column[] columns = columnList.toArray(new Column[0]);
                RowValue rowValue = new RowValue(0L, columns, i);

                bTreeMap.put(i, rowValue);
            }

            //bTreeMap.commitSaveDisk();

            long point1 = System.currentTimeMillis();
            System.out.println("存储" + count + "条数据，耗时:" + (point1 - startTime) / 1000 + "s");


            RowValue rowValue = bTreeMap.get(9999);
            long point2 = System.currentTimeMillis();

            List<Column> columnList = new ArrayList<>();
            Column c0 = new Column("id",  ColumnTypeEnum.INT_4.getColumnType(), 0, 4);
            Column c1 = new Column("name_1",  ColumnTypeEnum.VARCHAR.getColumnType(), 1, 36);
            Column c2 = new Column("age",  ColumnTypeEnum.INT_4.getColumnType(), 2, 4);
            columnList.add(c0);
            columnList.add(c1);
            columnList.add(c2);
            Column[] columns = columnList.toArray(new Column[0]);

            System.out.println("get result=" + rowValue.getRowEntity(columns));
            System.out.println("查询耗时:" + (point2 - point1) + "ms");



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            bpTreeStore.close();
        }
    }
}
