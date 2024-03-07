package test.parser.createTable;

import com.moyu.xmz.command.Command;
import com.moyu.xmz.command.QueryResult;
import com.moyu.xmz.command.ddl.CreateDatabaseCmd;
import com.moyu.xmz.command.ddl.DropDatabaseCmd;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Database;
import com.moyu.xmz.terminal.util.PrintResultUtil;
import test.annotation.TestCase;
import test.annotation.TestModule;

/**
 * @author xiaomingzhang
 * @date 2024/3/7
 */
@TestModule("创建表sql测试")
public class CreateTableSqlTest {

    private final static String databaseName = "create_table_test";

    private static Database database = null;

    static {
        DropDatabaseCmd dropDatabaseCmd = new DropDatabaseCmd(databaseName, true);
        dropDatabaseCmd.exec();
        CreateDatabaseCmd createDatabaseCmd = new CreateDatabaseCmd(databaseName);
        createDatabaseCmd.exec();
        database = Database.getDatabase(databaseName);
    }

    @TestCase("建表sql01")
    public void test01() {
        testExecSQL("create table test01_1(state int);");
        testExecSQL("create table test01_2(state int unsigned);");
        testExecSQL("create table test01_3(state int unsigned not null);");
        testExecSQL("create table test01_4(state int unsigned not null primary key);");
        testExecSQL("create table test01_5(state int unsigned not null primary key comment '状 态'));");
        testExecSQL("create table test01_6(state int unsigned not null default 1);");
        testExecSQL("create table test01_7(state int unsigned not null default 1 comment '状 态');");

        testExecSQL("create table test01_8(state int primary key);");
        testExecSQL("create table test01_9(state int primary key comment '状态 ');");
        testExecSQL("create table test01_10(state int primary key not null comment ' 状态');");
        testExecSQL("create table test01_11(state int not null comment ' 状态');");
        testExecSQL("create table test01_12(state int default null comment ' 状态');");
        testExecSQL("create table test01_13(state int comment ' 状态');");
    }

    @TestCase("建表sql02")
    public void test02() {
        testExecSQL("create table simple_01(id int,name varchar(64), state char(10), login_state tinyint);");
        testExecSQL("create table simple_02(id int primary key,name varchar(64) not null, state char(10) default '1', login_state tinyint);");
        testExecSQL("create table simple_03(id int primary key comment '主键',name varchar(64) not null comment '姓名', state char(10) default '1' comment '状态', login_state tinyint);");
        testExecSQL("create table simple_04(id int primary key comment '主 键',name varchar(64) not null comment '姓 名', state char(10) not null default '1' comment '状 态', login_state tinyint);");
    }

    @TestCase("建表sql03")
    private void test03() {
        testExecSQL("CREATE TABLE `sys_user` (\n" +
                "  `id` bigint(20) NOT NULL COMMENT 'ID',\n" +
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

    public static void main(String[] args) {
        new CreateTableSqlTest().test03();
    }

    private void testExecSQL(String sql) {
        System.out.println("====================================");
        System.out.println("执行语句 " + sql + "");
        ConnectSession connectSession = new ConnectSession(database);
        Command command = connectSession.prepareCommand(sql);
        QueryResult queryResult = command.exec();
        System.out.println("执行结果:");
        PrintResultUtil.printResult(queryResult);
        System.out.println("====================================");
    }



}
