package com.moyu.test.terminal;

import com.moyu.test.command.Command;
import com.moyu.test.command.Parser;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.SqlParser;
import com.moyu.test.net.model.terminal.DatabaseInfo;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.terminal.jline.TabCompleter;
import com.moyu.test.terminal.util.PrintResultUtil;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * @author xiaomingzhang
 * @date 2023/9/8
 * 使用Jline实现的命令行输入，支持补全功能/提示/光标移动等友好操作
 * 但是只linux系统环境可以生效
 */
public class JlineTerminal {

    public static void main(String[] args) {

        printDatabaseMsg();

        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {

            Parser parser = getSqlParser(null);
            LineReader reader = buildLineReader(terminal, null);

            while (true) {
                String inputStr = reader.readLine("yanySQL> ");
                // 退出命令
                if ("exit;".equals(inputStr) || "exit".equals(inputStr)) {
                    break;
                }

                if ("".equals(inputStr.trim())) {
                    continue;
                }

                try {
                    // 进入数据库
                    if (inputStr.startsWith("use ")) {
                        String dbName = inputStr.split("\\s+")[1];
                        Database database = Database.getDatabase(dbName);
                        if (database != null) {
                            parser = getSqlParser(database);
                            reader = buildLineReader(terminal, database);
                            System.out.println("ok");
                        } else {
                            System.out.println("数据库不存在");
                        }
                        continue;
                    }
                    // 执行命令
                    Command command = parser.prepareCommand(inputStr.trim());
                    QueryResult queryResult = command.execCommand();
                    PrintResultUtil.printResult(queryResult);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("结束");
    }

    private static LineReader buildLineReader(Terminal terminal, Database database) {
        return LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new TabCompleter(new DatabaseInfo(database)))
                .build();
    }

    private static Parser getSqlParser(Database database) {
        ConnectSession connectSession = new ConnectSession(database);
        return new SqlParser(connectSession);
    }

    private static void printDatabaseMsg() {
        System.out.println("+-------------------------------+");
        System.out.println("|       YanySQL1.0  (^_^)        |");
        System.out.println("+-------------------------------+");
        System.out.println("请输入命令...");
    }


}
