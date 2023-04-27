package test.command;

import com.moyu.test.command.QueryCommand;
import com.moyu.test.store.Chunk;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class QueryCommandTest {

    public static void main(String[] args) {
        String filePath = "D:\\mytest\\fileTest\\unfixLen.xmz";
        QueryCommand queryCommand = new QueryCommand(filePath);
        List<Chunk> execute = queryCommand.execute();
        execute.forEach(System.out::println);
    }
}
