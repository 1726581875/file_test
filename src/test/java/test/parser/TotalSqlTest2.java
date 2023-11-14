package test.parser;

import com.moyu.test.command.Command;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.ddl.CreateDatabaseCommand;
import com.moyu.test.command.ddl.DropDatabaseCommand;
import com.moyu.test.command.dml.InsertCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.CommonConstant;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.operation.OperateTableInfo;
import com.moyu.test.terminal.util.PrintResultUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/7/3
 */
public class TotalSqlTest2 {

    private static final String engineType = CommonConstant.ENGINE_TYPE_YAN;
    //private static final String engineType = CommonConstant.ENGINE_TYPE_YU;

    private final static String databaseName = "aaa";

    private static Database database = null;

    static {
        DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand(databaseName, true);
        dropDatabaseCommand.execute();
        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand(databaseName);
        createDatabaseCommand.execute();
        database = Database.getDatabase(databaseName);
    }


    public static void main(String[] args) {

        /*        fastInsertData2("table_100000", 100000, engineType);
        fastInsertData2("table_1000000", 1000000, engineType);*/

/*        yanStoreEngineTest();
        testCreateIndexCommand();
        fastInsertData2("abc_1", 10000, engineType);
        testOptimizeCondition();
        testOrderByQuery();
        deleteSqlTest();
        testCreateTable();

        testGroupBy();*/

        //testFunction();

        //testExecSQL("select uuid()");


        testExecSQL("drop table if exists  xmz_sort_test");
        testExecSQL("create table xmz_sort_test (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (3, '31', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (3, '32', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (3, '33', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (0, 'Daniel', '2023-06-29 15:45:00')");
        //testExecSQL("select count(*) from xmz_sort_test");
        //testExecSQL("select * from xmz_sort_test order by id desc, name asc");
        testExecSQL("select name,uuid() from xmz_sort_test order by id desc, name asc");

    }


    private static void testFunction() {
        testExecSQL("select uuid();");
        testExecSQL("select now();");
        testExecSQL("select now(),now(),uuid();");
        testExecSQL("select * from (select now() as a,now() b,uuid() as c) t");
        testExecSQL("select now() as a,now() b,uuid() as c;");
    }


    private static void testGroupBy() {
        testExecSQL("drop table if exists  groupby_test");
        testExecSQL("create table groupby_test (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (4, 'John', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (5, 'John', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (6, 'Sophia', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO groupby_test (id, name, time) VALUES (0, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("select * from groupby_test");
        testExecSQL("select id,name,count(*) from groupby_test group by id,name;");
        testExecSQL("select name,id,count(*) from groupby_test group by name,id");
        testExecSQL("select name,id,count(*) from groupby_test group by name,id limit 2");
    }




    private static void testCreateTable() {
        testExecSQL("CREATE TABLE `sys_user` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',\n" +
                "  `tenantId` varchar(36) NOT NULL COMMENT '租户id',\n" +
                "  `name` varchar(40) NOT NULL COMMENT '用户名称',\n" +
                "  `account` varchar(20) NOT NULL COMMENT '登录账号',\n" +
                "  `password` varchar(1048) NOT NULL COMMENT '登录密码',\n" +
                "  `status` tinyint(4) NOT NULL DEFAULT '1' COMMENT '用户状态| 1正常，2禁用，3已删除',\n" +
                "  `login_time` datetime DEFAULT NULL COMMENT '最近登录时间',\n" +
                "  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n" +
                "  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  UNIQUE KEY `uk_account` (`account`)\n" +
                ") ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COMMENT='用户表';");
    }


    private static void deleteSqlTest() {
        testExecSQL("drop table if exists  xmz_d_t");
        testExecSQL("create table xmz_d_t (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (3, '31', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (3, '32', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (3, '33', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_d_t (id, name, time) VALUES (0, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("select * from xmz_d_t");

        testExecSQL("delete from xmz_d_t where id = 6");

        testExecSQL("select * from xmz_d_t");
    }



    private static void testOrderByQuery(){
        testExecSQL("drop table if exists  xmz_sort_test");
        testExecSQL("create table xmz_sort_test (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (3, '31', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (3, '32', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (3, '33', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_sort_test (id, name, time) VALUES (0, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("select count(*) from xmz_sort_test");
        testExecSQL("select * from xmz_sort_test order by id desc, name asc");
        testExecSQL("select name from xmz_sort_test order by id desc, name asc");


        testExecSQL("drop table if exists  xmz_sort_test2");
        testExecSQL("create table xmz_sort_test2 (id int, name varchar(10), count int) ENGINE=" + engineType);

        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (1, 'John', 1)");
        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (2, 'Alice', 2)");
        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (3, '31', 1)");
        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (3, '32', 2)");
        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (3, '33', 3)");
        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (6, 'Sophia', 6)");
        testExecSQL("INSERT INTO xmz_sort_test2 (id, name, count) VALUES (0, 'Daniel', 7)");

        testExecSQL("select count(*) from xmz_sort_test2");
        testExecSQL("select * from xmz_sort_test2 order by id,count");
        testExecSQL("select * from xmz_sort_test2 order by id desc,count");
        testExecSQL("select * from xmz_sort_test2 order by id asc,count desc");
        testExecSQL("select * from xmz_sort_test2 order by id ,count desc");

        testExecSQL("select * from xmz_sort_test2 order by id desc ,count desc limit 2");
        testExecSQL("select * from xmz_sort_test2 order by id desc ,count desc");

    }

    private static void testOptimizeCondition(){
        testExecSQL("drop table if exists  xmz_q_2");
        testExecSQL("create table xmz_q_2 (id int primary key, name varchar(10), time timestamp) ENGINE=" + engineType);


        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (7, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (8, 'Olivia', '2023-06-29 16:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (9, 'David', '2023-06-29 17:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (10, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma1', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma2', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma3', '2022-06-29 18:00:00')");

        testExecSQL("select * from xmz_q_2 where 1 = 1 and 1 = 2");
        testExecSQL("select * from xmz_q_2 where 1 = 1 or id = 2");
        testExecSQL("select * from xmz_q_2 where id = 1 and id = 1 or id = 1");
        testExecSQL("select * from xmz_q_2 where id = 1 and 1 = 1 or id = 1");
        testExecSQL("select * from xmz_q_2 where id = 2 and 1 = 1 or id = 1");
        testExecSQL("select * from xmz_q_2 where id = 2 and id = 1");
        testExecSQL("select * from xmz_q_2 where id = 2 and id = 1");
    }



    private static void fastInsertData2(String tableName, int rowNum, String engineType) {
        testExecSQL("drop table if exists " + tableName);

        testExecSQL("create table "+ tableName +" (id int, name varchar(10), time timestamp) ENGINE=" + engineType);
        long beginTime = System.currentTimeMillis();
        long time = beginTime;

        List<Column[]> columnList = new ArrayList<>();
        ConnectSession connectSession = new ConnectSession(database);
        Column[] tableColumns = getColumns(null, null);
        OperateTableInfo tableInfo = new OperateTableInfo(connectSession, tableName, tableColumns, null);
        tableInfo.setEngineType(engineType);
        InsertCommand insertCommand = new InsertCommand(tableInfo, null);
        for (int i = 1; i <= rowNum; i++) {
            Column[] columns = getColumns(i, "name_" + i);
            columnList.add(columns);
            if (i % 10000 == 0) {
                insertCommand.batchWriteList(columnList);
                System.out.println("插入一万条记录耗时:" + (System.currentTimeMillis() - time) + "ms");
                time = System.currentTimeMillis();
                columnList.clear();
            }
        }

        insertCommand.batchWriteList(columnList);
        columnList.clear();

        testExecSQL("select count(*) from " + tableName);

        testExecSQL("desc " + tableName);
    }


    private static Column[] getColumns(Integer id, String name) {
        Column[] columns = new Column[3];
        // 字段11
        columns[0] = new Column("id", ColumnTypeEnum.INT.getColumnType(), 0, 4);
        //columns[0].setIsPrimaryKey((byte) 1);
        columns[0].setValue(id);

        // 字段2
        columns[1] = new Column("name", ColumnTypeEnum.VARCHAR.getColumnType(), 1, 100);
        columns[1].setValue(name);
        // 字段3
        columns[2] = new Column("time", ColumnTypeEnum.TIMESTAMP.getColumnType(), 2, 8);
        columns[2].setValue(new Date());
        return columns;
    }


    private static void testCreateIndexCommand(){
        testExecSQL("drop table if exists  xmz_q_2");
        testExecSQL("create table xmz_q_2 (id int primary key, name varchar(10), time timestamp) ENGINE=" + engineType);


        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (1, 'John', '2023-06-29 09:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (2, 'Alice', '2023-06-29 10:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (3, 'Mike', '2023-06-29 11:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (4, 'Emily', '2023-06-29 12:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (5, 'Tom', '2023-06-29 13:20:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (6, 'Sophia', '2023-06-29 14:10:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (7, 'Daniel', '2023-06-29 15:45:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (8, 'Olivia', '2023-06-29 16:30:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (9, 'David', '2023-06-29 17:15:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (10, 'Emma', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma1', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma2', '2023-06-29 18:00:00')");
        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (11, 'Emma3', '2022-06-29 18:00:00')");
        
        testExecSQL("select count(*) from xmz_q_2");
        testExecSQL("select * from xmz_q_2 where id = 5");

        //testExecSQL("create index idx_id on xmz_q_2(id)");

        testExecSQL("select * from xmz_q_2 where id = 5");
        testExecSQL("select * from xmz_q_2 where id = 11");

        testExecSQL("INSERT INTO xmz_q_2 (id, name, time) VALUES (12, 'Emma3', '2022-06-29 18:00:00')");
        testExecSQL("select * from xmz_q_2 where id = 12");


        testExecSQL("delete from xmz_q_2 where id = 12");
        testExecSQL("select * from xmz_q_2 where id = 12");

    }


    private static void yanStoreEngineTest() {
        testExecSQL("drop table if exists xmz_table");
        testExecSQL("create table xmz_table (id int primary key, name varchar(10)) ENGINE=" + engineType);
        testExecSQL("insert into  xmz_table (id, name) value (1, null);");
        testExecSQL("insert into  xmz_table (id, name) value (2, '摸鱼2');");
        testExecSQL("insert into  xmz_table (id, name) value (3, '摸鱼3');");
        testExecSQL("insert into  xmz_table (id, name) value (4, '摸鱼4');");

        testExecSQL("insert into  xmz_table (id, name) value (5, 'aaaa');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '啊啊啊');");
        testExecSQL("insert into  xmz_table (id, name) value (6, '摸鱼');");

        testExecSQL("select * from xmz_table where ((name = '摸鱼') and (name = '摸鱼') and (name = '摸鱼'))");
        testExecSQL("select * from xmz_table where ((name = '摸鱼'))");
        testExecSQL("select * from xmz_table where ((((name = '摸鱼'))))");
        testExecSQL("select * from xmz_table where ((((name = '摸鱼'))) or id = 1)");
        //testExecSQL("select count(*) from xmz_table where 1 = 1");
        //testExecSQL("select * from xmz_table where 1 = 1");
        //testExecSQL("select * from xmz_table where '1'= '1'");



        //testExecSQL("select * from xmz_table where (name = '摸鱼') and (name = '摸鱼')");
        //testExecSQL("select * from xmz_table where (((name = '摸鱼') and (name = '摸鱼') and (name = '摸鱼')) or 1 = 1)");
    }

    private static void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.execCommand();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }

}
