package com.moyu.test.terminal;

import com.moyu.test.net.model.terminal.DatabaseInfo;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.terminal.sender.TcpDataSender;
import com.moyu.test.terminal.util.PrintResultUtil;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 * 通过TCP连接进行查询的终端
 */
public class TcpTerminal {

    //private static final String ipAddress = "159.75.134.161";
    private static final String ipAddress = "localhost";

    private static final int port = 8888;

    private static final String END_CHAR = ";";

    public static void main(String[] args) {

        printDatabaseMsg();

        TcpDataSender tcpDataSender = new TcpDataSender(ipAddress, port);

        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            LineReader reader = buildLineReader(terminal, null);
            DatabaseInfo useDatabase = null;
            while (true) {
                // 输入sql
                String input = reader.readLine("yanySQL> ");
                if (!input.contains(END_CHAR) && !input.toUpperCase().startsWith("USE ")) {
                    String line = null;
                    while ((line = reader.readLine("       > ")) != null) {
                        input = input + " " + line;
                        if (input.contains(END_CHAR)) {
                            break;
                        }
                    }
                }
                String inputStr = input;
                // 退出命令
                if ("exit;".equals(inputStr) || "exit".equals(inputStr)) {
                    break;
                }

                if ("".equals(inputStr.trim())) {
                    continue;
                }

                try {
                    // 使用数据库,命令: use dbName
                    String[] split = getWords(inputStr);
                    if (split.length >= 2 && "USE".equals(split[0].toUpperCase())) {
                        String dbName = split[1];
                        useDatabase = tcpDataSender.getDatabaseInfo(dbName);
                        if (useDatabase != null) {
                            System.out.println("ok");
                            reader = buildLineReader(terminal, useDatabase);
                        } else {
                            System.out.println("数据库不存在");
                        }
                        continue;
                    }

                    // 如果还没使用数据库，只能执行show databases命令
                    if (useDatabase == null) {
                        String[] sqlSplit = getWords(inputStr);
                        if (split.length >= 2
                                && "SHOW".equals(sqlSplit[0].toUpperCase())
                                && ("DATABASES".equals(sqlSplit[1].toUpperCase())) || "DATABASES;".equals(sqlSplit[1].toUpperCase())) {
                            QueryResultDto queryResultDto = tcpDataSender.execQueryGetResult(-1, inputStr);
                            PrintResultUtil.printResult(queryResultDto);
                        } else {
                            System.out.println("请先使用use命令选择数据库..");
                        }
                    } else {
                        // 其他命令
                        QueryResultDto queryResultDto = tcpDataSender.execQueryGetResult(useDatabase.getDatabaseId(), inputStr);
                        PrintResultUtil.printResult(queryResultDto);
                    }

                } catch (Exception e) {
                    //System.out.println(e.getMessage());
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

    private static String[] getWords(String str) {
        List<String> workList = new ArrayList<>();
        char[] charArray = str.toCharArray();

        int start = 0;
        int end = 0;
        while (end < charArray.length) {
            if (charArray[end] == ' ' || charArray[end] == ';') {
                if (end > start) {
                    String word = str.substring(start, end).trim();
                    if (word.length() > 0) {
                        workList.add(word);
                    }
                    start = end + 1;
                }
                if(charArray[end] == ';') {
                    return workList.toArray(new String[0]);
                }
            }
            end++;
        }

        if(start < charArray.length) {
            String word = str.substring(start, charArray.length).trim();
            if (word.length() > 0) {
                workList.add(word);
            }
        }

        return workList.toArray(new String[0]);
    }


    private static void printDatabaseMsg() {
        System.out.println("+-------------------------------+");
        System.out.println("|       YanySQL1.0  (^_^)        |");
        System.out.println("+-------------------------------+");
        System.out.println("请输入命令...");
    }


}
