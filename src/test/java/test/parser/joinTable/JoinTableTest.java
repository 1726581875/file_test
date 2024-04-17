package test.parser.joinTable;

import com.moyu.xmz.command.dml.InsertCmd;
import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.TableInfo;
import test.annotation.TestCase;
import test.annotation.TestModule;
import test.parser.BaseSqlTest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/8/1
 */
@TestModule("多表连接测试")
public class JoinTableTest extends BaseSqlTest {

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;

    private final static String databaseName = "join_table";


    @TestCase("simpleJoinTest01")
    public void simpleJoinTest01() {

/*        fastInsertData("table_1", 1000, engineType);
        fastInsertData("table_2", 1000, engineType);
        //testExecSQL("create index idx_id on table_2(id)");
        testExecSQL("select count(*) from table_1 a inner join table_2 b on a.id = b.id");*/


        testExecSQL("drop table if exists tb_a");
        testExecSQL("create table tb_a (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO tb_a (id, name, time) VALUES (1, 'John-a', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO tb_a (id, name, time) VALUES (2, 'Alice-a', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO tb_a (id, name, time) VALUES (3, 'Mike-a', '2023-06-29 11:15:00')");

        testExecSQL("drop table if exists tb_b");
        testExecSQL("create table tb_b (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO tb_b (id, name, time) VALUES (1, 'John-b', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO tb_b (id, name, time) VALUES (2, 'Alice-b', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO tb_b (id, name, time) VALUES (4, 'Mike-b', '2023-06-29 11:15:00')");

        testExecSQL("drop table if exists tb_c");
        testExecSQL("create table tb_c (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO tb_c (id, name, time) VALUES (1, 'John-c', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO tb_c (id, name, time) VALUES (2, 'Alice-c', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO tb_c (id, name, time) VALUES (3, 'Mike-c', '2023-06-29 11:15:00')");

        testExecSQL("drop table if exists tb_d");
        testExecSQL("create table tb_d (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO tb_d (id, name, time) VALUES (4, 'John-d', '2023-06-29 09:30:00')");


        testExecSQL("select * from tb_a as a " +
                "left join tb_b as b on a.id = b.id " +
                "left join tb_c as c on c.id = a.id left join tb_d as d on d.id = b.id");
    }


    public void simpleJoinTest02() {

        testExecSQL("drop table if exists xmz_q_1");

        testExecSQL("create table xmz_q_1 (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (7, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (8, 'Olivia', '2023-06-29 16:30:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (9, 'David', '2023-06-29 17:15:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (10, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (11, 'Emma1', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (11, 'Emma2', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_1 (id, name, time) VALUES (11, 'Emma3', '2022-06-29 18:00:00')");

        testExecSQL("drop table if exists  xmz_q_2");
        testExecSQL("create table xmz_q_2 (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");

/*        // 内连接
        testExecSQL("select * from xmz_q_2 a inner join xmz_q_1 b on a.id = b.id");
        // 左连接
        testExecSQL("select * from xmz_q_1 a left join xmz_q_2 b on a.id = b.id");
        // 右连接
        testExecSQL("select * from xmz_q_2 a right join xmz_q_1 b on a.id = b.id");*/


        //testExecSQL("select * from xmz_q_2 a , xmz_q_1 b where a.id = b.id");
        testExecSQL("select * from xmz_q_2 a , xmz_q_1 b where a.id = b.id and a.id = 1");

        //testExecSQL("select count(*) from xmz_q_2 a inner join xmz_q_1 b on 1=1 or a.id = b.id");
    }

    @TestCase("fastInsertData")
    private void fastInsertData(String tableName, int rowNum, String engineType) {
        testExecSQL("drop table if exists " + tableName);

        testExecSQL("create table "+ tableName +" (id int primary key, name varchar(10), time timestamp) ENGINE=" + engineType);

        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        ConnectSession connectSession = new ConnectSession(database);
        Column[] tableColumns = database.getTable(tableName).getColumns();
        TableInfo tableInfo = new TableInfo(connectSession, tableName, tableColumns, null);
        tableInfo.setEngineType(engineType);
        InsertCmd insertCmd = new InsertCmd(tableInfo, null);
        for (int i = 1; i <= rowNum; i++) {
            Column[] columns = getColumns(i, "name_" + i);
            columnList.add(columns);
            if (i % 10000 == 0) {
                insertCmd.batchWriteList(columnList);
                System.out.println("插入一万条记录耗时:" + (System.currentTimeMillis() - time) + "ms");
                time = System.currentTimeMillis();
                columnList.clear();
            }
        }

        insertCmd.batchWriteList(columnList);
        columnList.clear();

        testExecSQL("select count(*) from " + tableName);

        testExecSQL("desc " + tableName);
    }

    private static Column[] getColumns(Integer id, String name) {
        Column[] columns = new Column[3];
        // 字段11
        columns[0] = new Column("id", ColumnTypeEnum.INT.getColumnType(), 0, 4);
        columns[0].setIsPrimaryKey((byte) 1);
        columns[0].setValue(id);

        // 字段2
        columns[1] = new Column("name", ColumnTypeEnum.VARCHAR.getColumnType(), 1, 100);
        columns[1].setValue(name);
        // 字段3
        columns[2] = new Column("time", ColumnTypeEnum.TIMESTAMP.getColumnType(), 2, 8);
        columns[2].setValue(new Date());
        return columns;
    }


    @Override
    protected Database initDatabase() {
        return createDatabase(databaseName);
    }

}
