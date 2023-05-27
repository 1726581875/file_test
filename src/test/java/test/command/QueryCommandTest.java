package test.command;

import com.moyu.test.command.condition.Condition;
import com.moyu.test.constant.OperatorConstant;
import test.readwrite.entity.Chunk;

import java.util.Arrays;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public class QueryCommandTest {

    public static void main(String[] args) {
        String filePath = "D:\\mytest\\fileTest\\unfixLen.xmz";
        TestQueryCommand queryCommand = new TestQueryCommand(filePath);
        queryCommand.setOffset(10);
        queryCommand.setLimit(100);

        // 设置查询条件
        Condition condition = new Condition();
        condition.setKey(null);
        condition.setValue(Arrays.asList("Hello World 11"));
        condition.setOperator(OperatorConstant.NOT_EQUAL_1);
        queryCommand.setCondition(condition);

        // 执行命令
        List<Chunk> execute = queryCommand.execute();
        execute.forEach(System.out::println);
    }
}
