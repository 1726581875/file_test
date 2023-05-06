package test.command;

import com.moyu.test.command.ddl.CreateDatabaseCommand;
import com.moyu.test.command.ddl.ShowDatabasesCommand;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class DDLCommandTest {

    public static void main(String[] args) {

        // 创建数据库
        CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand();
        createDatabaseCommand.setDatabaseName("database_01");
        createDatabaseCommand.execute();

        // 查询所有数据库
        ShowDatabasesCommand showDatabasesCommand = new ShowDatabasesCommand();
        List<String> result = showDatabasesCommand.execute();
        result.forEach(System.out::println);
    }


}
