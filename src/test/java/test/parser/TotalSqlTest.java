package test.parser;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.SqlParser;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
import com.moyu.xmz.command.dml.InsertCmd;
import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.TableInfo;
import com.moyu.xmz.terminal.util.PrintResultUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/9
 */
public class TotalSqlTest {

    private final static String databaseName = "xmz";

    private static Database database = null;

    static {
        DropDatabaseCmd dropDatabaseCmd = new DropDatabaseCmd(databaseName, true);
        dropDatabaseCmd.execCommand();
        CreateDatabaseCmd createDatabaseCmd = new CreateDatabaseCmd(databaseName);
        createDatabaseCmd.execCommand();
        database = Database.getDatabase(databaseName);
    }


    public static void main(String[] args) {
        testDatabaseDDL();
        testTableDDL();
        testInsert();
        testSimpleSelect();
        testUseIndex();
        testJoinTable();
        testSubQuery();
        testFunction();
        testSubQuery2();
        testAlias();
        testSubQueryFunction();
        testSubQueryFunction2();
        testRangeQuery();
        testRangeIndexQuery();
        testSubQueryTempTableToDisk();
        testLikeString();
        gptRandom20Test();
        gptRandom20Test2();
        yanStoreEngineTest();
        //bigTableJoin();
        testUseIndex();
    }


    private static void bigTableJoin(){
        fastInsertData2("y_y_1", 100000, CommonConstant.ENGINE_TYPE_YAN);
        fastInsertData2("y_y_2", 1000, CommonConstant.ENGINE_TYPE_YAN);
        testExecSQL("select count(*) from y_y_1 a inner join y_y_2 b on a.id = b.id");
        testExecSQL("select count(*) from y_y_2 a inner join y_y_1 b on a.id = b.id");
        //testExecSQL("select * from y_y_2 a inner join y_y_1 b on a.id = b.id");


        fastInsertData2("y_y_3", 100000, CommonConstant.ENGINE_TYPE_YU);
        fastInsertData2("y_y_4", 1000, CommonConstant.ENGINE_TYPE_YU);

        testExecSQL("select count(*) from y_y_3 a inner join y_y_4 b on a.id = b.id");
        testExecSQL("select count(*) from y_y_4 a inner join y_y_3 b on a.id = b.id");
    }




    private static void yanStoreEngineTest() {
        testExecSQL("drop table if exists xmz_table");
        testExecSQL("create table xmz_table (id int, name varchar(10)) ENGINE=yanStore");
        testExecSQL("insert into  xmz_table (id, name) value (1, null);");
        testExecSQL("insert into  xmz_table (id, name) value (2, '摸鱼2');");
        testExecSQL("insert into  xmz_table (id, name) value (3, '摸鱼3');");
        testExecSQL("insert into  xmz_table (id, name) value (4, '摸鱼4');");

        testExecSQL("insert into  xmz_table (id, name) value (5, 'aaaa');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '啊啊啊');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '摸鱼');");

        testExecSQL("select * from xmz_table where (name = '摸鱼') and (name = '摸鱼')");
        testExecSQL("select * from xmz_table");
        testExecSQL("select count(*) from xmz_table");
    }




    private static void gptRandom20Test2() {
        testExecSQL("drop table if exists  xmz_o_2");
        testExecSQL("create table xmz_o_2 (id int, name varchar(10), time timestamp)");


        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (7, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (8, 'Olivia', '2023-06-29 16:30:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (9, 'David', '2023-06-29 17:15:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (10, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (11, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (11, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (11, 'Emma', '2022-06-29 18:00:00')");

/*        testExecSQL("SELECT COUNT(DISTINCT name) FROM xmz_o_2");
        testExecSQL("SELECT COUNT(name) FROM xmz_o_2");*/

        testExecSQL("SELECT DISTINCT name FROM xmz_o_2");
        testExecSQL("SELECT DISTINCT name,id FROM xmz_o_2");
        testExecSQL("SELECT DISTINCT name,id,time FROM xmz_o_2");

/*        testExecSQL("SELECT * FROM xmz_o_2 WHERE id = 5");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name = 'John'");
        testExecSQL("SELECT id, name FROM xmz_o_2 WHERE id > 3 AND id < 8");
        testExecSQL("SELECT COUNT(*) FROM xmz_o_2");
        testExecSQL("SELECT MAX(time) FROM xmz_o_2");
        testExecSQL("SELECT name FROM xmz_o_2 ORDER BY id DESC LIMIT 3");
        //testExecSQL("SELECT * FROM xmz_o_2 WHERE DATE(time) = '2023-06-29'");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE id IN (2, 4, 6)");
        testExecSQL("SELECT AVG(id) FROM xmz_o_2");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name LIKE '%a%'");

        testExecSQL("SELECT id, name FROM xmz_o_2 WHERE id BETWEEN 3 AND 7");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name = 'Tom' AND time >= '2023-06-29 13:00:00'");
        //testExecSQL("SELECT DISTINCT name FROM xmz_o_2");
        testExecSQL("SELECT * FROM xmz_o_2 ORDER BY time ASC");
        testExecSQL("SELECT AVG(id) FROM xmz_o_2 WHERE time < '2023-06-29 15:00:00'");
        //testExecSQL("SELECT COUNT(DISTINCT name) FROM xmz_o_2");
        //testExecSQL("SELECT * FROM xmz_o_2 WHERE YEAR(time) = 2023");
        testExecSQL("SELECT id, name FROM xmz_o_2 WHERE id > (SELECT MAX(id) FROM xmz_o_2)");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name LIKE 'D%'");
        testExecSQL("SELECT MIN(time) FROM xmz_o_2");*/
    }


    private static void testLikeString() {
        testExecSQL("drop table if exists  xmz_o_2");
        testExecSQL("create table xmz_o_2 (id int, name varchar(10), time timestamp)");

        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (1, 'John', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (2, 'Alice', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (3, 'Bob', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (4, 'Charlie', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (5, 'Emily', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (6, 'David', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (7, 'Emma', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (8, 'Oliver', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (9, 'Sophia', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (10, 'James', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_2 (id, name, time) VALUES (11, 'Isabella', '2023-06-26 01:24:38')");

        testExecSQL("SELECT * FROM xmz_o_2 WHERE name like 'Isabella'");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name like '%abella'");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name like 'Is%'");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name like 'Is1%'");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name not like 'Is%'");
        testExecSQL("SELECT * FROM xmz_o_2 WHERE name like 'Isab%lla'");
    }


    private static void gptRandom20Test() {
        testExecSQL("drop table if exists  xmz_o_1");
        testExecSQL("create table xmz_o_1 (id int, name varchar(10), time timestamp)");


        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (1, 'John', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (2, 'Alice', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (3, 'Bob', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (4, 'Charlie', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (5, 'Emily', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (6, 'David', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (7, 'Emma', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (8, 'Oliver', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (9, 'Sophia', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (10, 'James', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (11, 'Isabella', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (12, 'Liam', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (13, 'Mia', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (14, 'Benjamin', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (15, 'Charlotte', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (16, 'Henry', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (17, 'Ava', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (18, 'Ethan', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (19, 'Amelia', '2023-06-26 01:24:38')");
        testExecSQL("INSERT INTO xmz_o_1 (id, name, time) VALUES (20, 'Noah', '2023-06-26 01:24:38')");

        testExecSQL("SELECT * FROM xmz_o_1 WHERE id <= 10");
        testExecSQL("SELECT name FROM xmz_o_1 WHERE id >= 100");
        testExecSQL("SELECT id, time FROM xmz_o_1 WHERE name = 'John'");
        testExecSQL("SELECT COUNT(*) FROM xmz_o_1 WHERE time >= '2023-01-01 00:00:00' AND time <= '2023-06-30 23:59:59'");
        //testExecSQL("SELECT DISTINCT name FROM xmz_o_1 ORDER BY time DESC");
        testExecSQL("SELECT * FROM xmz_o_1 LIMIT 5");
        testExecSQL("SELECT name FROM xmz_o_1 WHERE id IN (1, 3, 5, 7, 9)");
        testExecSQL("SELECT AVG(id) FROM xmz_o_1 WHERE time >= '2023-01-01 00:00:00' AND time <= '2023-06-30 23:59:59'");
        testExecSQL("SELECT * FROM xmz_o_1 WHERE name LIKE '%apple%'");
        testExecSQL("SELECT MAX(id) FROM xmz_o_1");
        testExecSQL("SELECT id FROM xmz_o_1 WHERE name IS NOT NULL");
        testExecSQL("SELECT * FROM xmz_o_1 WHERE time BETWEEN '2023-01-01 00:00:00' AND '2023-06-30 23:59:59'");
        testExecSQL("SELECT MIN(time) FROM xmz_o_1");
        testExecSQL("SELECT * FROM xmz_o_1 WHERE id > (SELECT MAX(id) FROM xmz_o_1)");
        //testExecSQL("SELECT name, COUNT(*) FROM xmz_o_1 GROUP BY name HAVING COUNT() > 1");
        //testExecSQL("SELECT * FROM xmz_o_1 WHERE id = (SELECT MAX(id) FROM xmz_o_1)");
        //testExecSQL("SELECT YEAR(time), COUNT(*) FROM xmz_o_1 GROUP BY YEAR(time)");
        testExecSQL("SELECT * FROM xmz_o_1 WHERE name IN ('Alice', 'Bob', 'Charlie')");
        //testExecSQL("SELECT DATEDIFF(NOW(), time) AS diff FROM xmz_o_1");
        //testExecSQL("SELECT id, CASE WHEN name IS NULL THEN 'Unknown' ELSE name END AS name FROM xmz_o_1");
    }


    private static void testSubQueryTempTableToDisk() {
        fastInsertData("xmz_s_1", 10000);
        testExecSQL("select count(*) from xmz_s_1 where id in (select id from xmz_s_1)");
        testExecSQL("select * from xmz_s_1 where id not in (select id from xmz_s_1)");
    }


    private static void testRangeIndexQuery() {
        fastInsertData("xmz_o_1", 10000);

        testExecSQL("create index idx_o_id on xmz_o_1(id);");

        testExecSQL("select * from xmz_o_1 where id <= 10");
        testExecSQL("select * from xmz_o_1 where id < 10");
        testExecSQL("select * from xmz_o_1 where id >= 10 and id <= 100");
        testExecSQL("select * from xmz_o_1 where id >= 9998");

        testExecSQL("select count(*) from xmz_o_1 where id < 500");
        testExecSQL("select count(*) from xmz_o_1 where id >= 500");
        testExecSQL("select count(*) from xmz_o_1 where id < 0");
        testExecSQL("select count(*) from xmz_o_1 where id < -1");
        testExecSQL("select count(*) from xmz_o_1 where id < 666 and id > 555");

        testExecSQL("select count(*) from xmz_o_1 where id >= 9998");
        testExecSQL("select * from xmz_o_1 where id = 9998");
        testExecSQL("select count(*) from xmz_o_1 where id <= 10000");
        testExecSQL("select count(*) from xmz_o_1 where id >= 0");

    }



    private static void fastInsertData2(String tableName, int rowNum, String engineType) {
        testExecSQL("drop table if exists " + tableName);

        testExecSQL("create table "+ tableName +" (id int primary key, name varchar(10), time timestamp) ENGINE=" + engineType);
        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        ConnectSession connectSession = new ConnectSession(database);
        Column[] tableColumns = getColumns(null, null);
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
        System.out.println("插入一万条记录耗时:" + (System.currentTimeMillis() - time) + "ms");
        columnList.clear();

        testExecSQL("select count(*) from " + tableName);

        testExecSQL("desc " + tableName);
    }

    private static void fastInsertData(String tableName, int rowNum) {
        testExecSQL("drop table if exists " + tableName);

        testExecSQL("create table "+ tableName +" (id int, name varchar(10), time timestamp)");
        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        ConnectSession connectSession = new ConnectSession(database);
        Column[] tableColumns = getColumns(null, null);
        TableInfo tableInfo = new TableInfo(connectSession, tableName, tableColumns, null);
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
        System.out.println("插入一万条记录耗时:" + (System.currentTimeMillis() - time) + "ms");
        time = System.currentTimeMillis();
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

    public static void testRangeQuery(){
        testExecSQL("drop table if exists  xmz_y_3");
        testExecSQL("create table xmz_y_3 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_y_3(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_3(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_3(id,name,time) value (3,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_3(id,name,time) value (4,'444','2023-05-19 00:00:00')");


        testExecSQL("select * from xmz_y_3 where id < 3");
        testExecSQL("select count(*) from xmz_y_3 where id < 3");
        testExecSQL("select  *  from xmz_y_3 where id >= 3");
        testExecSQL("select  *  from xmz_y_3 where id > 3");
        testExecSQL("select  *  from xmz_y_3 where id <= 3");
        testExecSQL("select  *  from xmz_y_3 where id <= 3 and id > 1");
        testExecSQL("select  *  from xmz_y_3 where id between 1 and 2");
    }


    public static void testSubQueryFunction2(){
        testExecSQL("drop table if exists  xmz_y_2");
        testExecSQL("create table xmz_y_2 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_y_2(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_2(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_2(id,name,time) value (3,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_2(id,name,time) value (1,'444','2023-05-19 00:00:00')");


        testExecSQL("select * from xmz_y_2");
        testExecSQL("select count(*) from xmz_y_2");
        testExecSQL("select id,count(*) from xmz_y_2 group by id");
        testExecSQL("select * from (select id,count(*) from xmz_y_2 group by id) t");
        testExecSQL("select * from (select id,count(*),max(id) from xmz_y_2 group by id) t");
    }


    public static void testSubQueryFunction() {

        testExecSQL("drop table  if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select max(id),min(id),count(*) from xmz_yan) t");
        testExecSQL("select * from (select max(id) as a,min(id) as b,count(*) as c from xmz_yan) t");
        testExecSQL("select * from (select max(id) a,min(id) b,count(*) c from xmz_yan) t");
        testExecSQL("select t.* from (select max(id) a,min(id) b,count(*) c from xmz_yan) t");

        testExecSQL("select t.a,t.b from (select max(id) a,min(id) b,count(*) c from xmz_yan) t");

    }


    private static void testAlias(){

        testExecSQL("drop table if exists xmz_y_1");
        testExecSQL("create table xmz_y_1(id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_y_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_y_1(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select y.* from xmz_y_1 as y where y.id in (1)");
        testExecSQL("select * from xmz_y_1 as y where y.id in (1)");
        testExecSQL("select id,name,time from xmz_y_1 as y where y.id in (1)");
        testExecSQL("select id,name,time from xmz_y_1 where id in (1)");
        testExecSQL("select xmz.id,xmz.name,xmz.time from xmz_y_1 xmz");
        testExecSQL("select id as id1,name as name2 ,time as time3 from xmz_y_1 where id in (1)");
        testExecSQL("select bb.id as id1,bb.name as name2 ,bb.time as time3 from xmz_y_1 as bb");

        testExecSQL("select bb.id as id1, bb.* from xmz_y_1 as bb");
    }



    private static void testSubQuery2(){
        testExecSQL("drop table if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select * from xmz_yan) t");
        testExecSQL("select * from (select * from xmz_yan a where a.id = 1) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id where b.id = 1 ) t where id = 1");
        testExecSQL("select * from (select * from xmz_yan where id = 1) t");
        testExecSQL("select * from (select * from xmz_yan ) t where t.id = 1");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1 ) t0 ) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");
        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");


        testExecSQL("select * from xmz_yan where id in (1)");
        testExecSQL("select * from xmz_yan where name in ('222')");
        testExecSQL("select * from xmz_yan where name not in ('222')");
        testExecSQL("select * from xmz_yan where id in (select id from xmz_yan)");
    }



    private static void testFunction(){

        testExecSQL("create table table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("desc table_1");

        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into table_1(id,name,time) value (1,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from table_1");

        testExecSQL("update table_1 set name = 'aaa' where id = 1");

        testExecSQL("select * from table_1");


        testExecSQL("select count(*) from table_1");
        testExecSQL("select sum(id) from table_1");
        testExecSQL("select max(id) from table_1");
        testExecSQL("select min(id) from table_1");

        testExecSQL("select count(*),sum(id) from table_1");
        testExecSQL("select max(id),sum(id) from table_1");
        testExecSQL("select min(id),sum(id) from table_1");
        testExecSQL("select max(id),min(id) from table_1");

        testExecSQL("select count(time) from table_1 where id = 2");
        //testExecSQL("select sum(time) from table_1 where id = 2");
        testExecSQL("select max(time) from table_1 where id = 2");
        testExecSQL("select min(time) from table_1 where id = 2");


        testExecSQL("truncate table table_1");

        testExecSQL("drop table if exists  table_1");
    }


    public static void testSubQuery() {

        testExecSQL("drop table  if exists  xmz_yan");
        testExecSQL("create table xmz_yan (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_yan(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_yan(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("select * from (select * from xmz_yan) t");
        testExecSQL("select * from (select * from xmz_yan where id = 1) t");
        testExecSQL("select * from (select * from xmz_yan ) t where t.id = 1");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1 ) t0 ) t");
        testExecSQL("select * from (select * from (select * from xmz_yan where id = 1) t0 where t0.id = 1 ) t");


        testExecSQL("select * from (select * from xmz_yan as a left join xmz_yan as b on a.id = b.id ) t where id = 1");


    }


    public static void testJoinTable() {

        testExecSQL("drop table  if exists  xmz_00");
        testExecSQL("create table xmz_00 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_00(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_00(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_00(id,name,time) value (3,'222','2023-05-19 00:00:00')");

        testExecSQL("drop table if exists xmz_01");
        testExecSQL("create table xmz_01 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_01(id,name,time) value (1,'111.','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_01(id,name,time) value (4,'444.','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_01(id,name,time) value (5,'555.','2023-05-19 00:00:00')");


        testExecSQL("drop table if exists xmz_02");
        testExecSQL("create table xmz_02 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_02(id,name,time) value (1,'111.2','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_02(id,name,time) value (3,'222.2','2023-05-19 00:00:00')");


        testExecSQL("select * from xmz_00 as a inner join xmz_01 as b on a.id = b.id inner join xmz_02 as c on a.id = c.id");


        testExecSQL("select * from xmz_00 a right join xmz_01 b on a.id = b.id");
        testExecSQL("select * from xmz_00 a left join xmz_01 b on a.id = b.id");
        testExecSQL("select * from xmz_00 a inner join xmz_01 b on a.id = b.id");
    }



    public static void testUseIndex() {
        testExecSQL("drop table if exists xmz_table_1");

        testExecSQL("create table xmz_table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_table_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (3,'333','2023-05-19 00:00:00')");

        testExecSQL("create index id_idx on xmz_table_1(id)");

        testExecSQL("select * from xmz_table_1 where id = 3");
    }



    public static void testSimpleSelect() {

        testExecSQL("drop table if exists xmz_table");
        testExecSQL("create table xmz_table (id int, name varchar(10))");

        testExecSQL("insert into  xmz_table (id, name) value (1, null);");
        testExecSQL("insert into  xmz_table (id, name) value (2, '摸鱼2');");
        testExecSQL("insert into  xmz_table (id, name) value (3, '摸鱼3');");
        testExecSQL("insert into  xmz_table (id, name) value (4, '摸鱼4');");

        testExecSQL("insert into  xmz_table (id, name) value (5, 'aaaa');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '啊啊啊');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '摸鱼');");

        testExecSQL("select * from xmz_table where (name = '摸鱼') and (name = '摸鱼')");
        testExecSQL("select * from xmz_table where (name = '摸鱼' and name = '摸鱼123')");
        testExecSQL("select * from xmz_table where (name = '摸鱼' or name = '摸鱼')");
        testExecSQL("select * from xmz_table");
        testExecSQL("select * from xmz_table where name = '摸鱼' or (id = 1)");
        //testExecSQL("select * from xmz_table where (((name = '摸鱼') or (id = 1)))");

        testExecSQL("select * from xmz_table where name like 摸鱼");
        testExecSQL("select * from xmz_table where name is null");
        testExecSQL("select * from xmz_table where name is not null");


    }


    public static void testInsert() {
        testExecSQL("drop table if exists xmz_table_1");
        testExecSQL("create table xmz_table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("insert into xmz_table_1(id,name,time) value (1,'111','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (2,'222','2023-05-19 00:00:00')");
        testExecSQL("insert into xmz_table_1(id,name,time) value (3,'333','2023-05-19 00:00:00')");

    }


    private static void testTableDDL() {

        testExecSQL("drop table if exists xmz_table_1");
        testExecSQL("drop table if exists xmz_table_3");
        testExecSQL("drop table if exists xmz_table_4");

        testExecSQL("create table xmz_table_1 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_table_2 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_table_3 (id int, name varchar(10), time timestamp)");
        testExecSQL("create table xmz_table_4 (id int, name varchar(10), time timestamp)");
        testExecSQL("drop table xmz_table_2");
        testExecSQL("show tables");

        testExecSQL("desc xmz_table_3");

    }


    private static void testDatabaseDDL() {
        testExecSQL("show databases");
        testExecSQL("create database xmz_01");
        testExecSQL("create database xmz_02");
        testExecSQL("create database xmz_03");
        testExecSQL("drop database xmz_02");
        testExecSQL("show databases");
        testExecSQL("drop database xmz_01");
        testExecSQL("drop database xmz_03");
    }



    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        SqlParser sqlParser = new SqlParser(connectSession);
        Command command = sqlParser.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }

}
