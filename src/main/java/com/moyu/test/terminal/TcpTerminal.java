package com.moyu.test.terminal;

import com.moyu.test.command.Parser;
import com.moyu.test.command.SqlParser;
import com.moyu.test.exception.DbException;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.net.model.terminal.DatabaseInfo;
import com.moyu.test.terminal.sender.TcpDataSender;
import com.moyu.test.terminal.util.PrintResultUtil;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class TcpTerminal {

    //private static final String ipAddress = "159.75.134.161";
    private static final String ipAddress = "localhost";

    private static final int port = 8888;

    public static void main(String[] args) {

        printDatabaseMsg();

        TcpDataSender tcpDataSender = new TcpDataSender(ipAddress, port);

        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            LineReader reader = buildLineReader(terminal, null);
            DatabaseInfo useDatabase = null;
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
                    String[] split = inputStr.trim().split("\\s+");
                    if (split.length >= 2 && "USE".equals(split[0].toUpperCase())) {
                        String dbName = inputStr.split("\\s+")[1];
                        useDatabase = tcpDataSender.getDatabaseInfo(dbName);
                        if (useDatabase != null) {
                            System.out.println("ok");
                            reader = buildLineReader(terminal, useDatabase);
                        } else {
                            System.out.println("数据库不存在");
                        }
                        continue;
                    }


                    // show databases命令
                    if(useDatabase == null) {
                        String sql = inputStr.trim();
                        String[] sqlSplit = inputStr.split("\\s+");
                        if(split.length >= 2
                                && "SHOW".equals(sqlSplit[0].toUpperCase())
                                &&  "DATABASES".equals(sqlSplit[1].toUpperCase())) {
                            QueryResultDto queryResultDto = tcpDataSender.execQueryGetResult(-1, inputStr);
                            if(queryResultDto == null) {
                                throw new DbException("发生异常，结果为空");
                            } else {
                                PrintResultUtil.printResult(queryResultDto);
                                continue;
                            }
                        }
                        System.out.println("请先使用use命令选择数据库..");
                        continue;
                    }

                    QueryResultDto queryResultDto = tcpDataSender.execQueryGetResult(useDatabase.getDatabaseId(), inputStr);
                    if(queryResultDto == null) {
                        throw new DbException("发生异常，结果为空");
                    } else {
                        PrintResultUtil.printResult(queryResultDto);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("结束");
    }

    private static LineReader buildLineReader(Terminal terminal, DatabaseInfo database) {
        return LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new TabCompleter(database))
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
