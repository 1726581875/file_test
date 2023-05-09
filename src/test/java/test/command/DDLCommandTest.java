package test.command;

import com.moyu.test.command.ddl.CreateDatabaseCommand;
import com.moyu.test.command.ddl.DropDatabaseCommand;
import com.moyu.test.command.ddl.ShowDatabasesCommand;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class DDLCommandTest {

    public static void main(String[] args) {

        for(int i = 0; i < 10; i++) {

            String databaseName = "database_" + i;
            // 创建数据库
            CreateDatabaseCommand createDatabaseCommand = new CreateDatabaseCommand();
            createDatabaseCommand.setDatabaseName(databaseName);
            createDatabaseCommand.execute();

            // 删除数据库测试
            if(i % 2 == 0) {
                DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand();
                dropDatabaseCommand.setDatabaseName(databaseName);
                dropDatabaseCommand.execute();
            }
        }

        // 查询所有数据库
        ShowDatabasesCommand showDatabasesCommand = new ShowDatabasesCommand();
        List<String> result = showDatabasesCommand.execute();
        result.forEach(System.out::println);

        // 删除所有数据库
        result.forEach(databaseName -> {
            DropDatabaseCommand dropDatabaseCommand = new DropDatabaseCommand();
            dropDatabaseCommand.setDatabaseName(databaseName);
            dropDatabaseCommand.execute();
        });

        System.out.println("======= 删除所有后 ======");
        List<String> result2 = showDatabasesCommand.execute();
        result2.forEach(System.out::println);
    }


}
