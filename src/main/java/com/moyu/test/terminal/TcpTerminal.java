package com.moyu.test.terminal;

import com.moyu.test.net.constant.CommandTypeConstant;
import com.moyu.test.net.model.BaseResultDto;
import com.moyu.test.net.model.terminal.DatabaseInfo;
import com.moyu.test.net.model.terminal.QueryResultDto;
import com.moyu.test.net.packet.ErrPacket;
import com.moyu.test.net.packet.OkPacket;
import com.moyu.test.net.packet.Packet;
import com.moyu.test.net.util.ReadWriteUtil;
import com.moyu.test.terminal.sender.TcpDataSender;
import com.moyu.test.terminal.util.PrintResultUtil;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 * 通过TCP连接进行查询的终端
 * 本地运行先启动TcpServer，再运行该类
 */
public class TcpTerminal {

    private static final String ipAddress = "159.75.134.161";
    //private static final String ipAddress = "localhost";

    private static final int port = 8888;

    private static final String END_CHAR = ";";

    private static final int PRINT_LIMIT = 1000;

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
                String[] inputWords = getWords(inputStr);
                if (inputWords.length > 0 && ("EXIT".equals(inputWords[0].toUpperCase())
                        || "QUIT".equals(inputWords[0].toUpperCase()))) {
                    break;
                }

                if ("".equals(inputStr.trim())) {
                    continue;
                }

                try {
                    // 使用数据库,命令: use dbName
                    if (inputWords.length >= 2 && "USE".equals(inputWords[0].toUpperCase())) {
                        String dbName = inputWords[1];
                        useDatabase = tcpDataSender.getDatabaseInfo(dbName);
                        if (useDatabase != null) {
                            System.out.println("ok");
                            reader = buildLineReader(terminal, useDatabase);
                        } else {
                            System.out.println("数据库不存在");
                        }
                        continue;
                    }

                    // 如果还没使用数据库，只能执行show databases/create database命令
                    if (useDatabase == null) {
                        if (isShowDatabases(inputWords) || isCreateDatabase(inputWords)) {
                            QueryResultDto queryResultDto = tcpDataSender.execQueryCommand(-1, inputStr);
                            PrintResultUtil.printResult(queryResultDto);
                        } else {
                            System.out.println("请先使用use命令选择数据库..");
                        }
                        continue;
                    }

                    // 查询命令
                    if (inputWords.length > 1 && "SELECT".equals(inputWords[0].toUpperCase())) {
                        execSelectCommandGetResult(useDatabase.getDatabaseId(), inputStr, reader);
                    } else {
                        QueryResultDto queryResultDto = tcpDataSender.execQueryCommand(useDatabase.getDatabaseId(), inputStr);
                        if (queryResultDto == null) {
                            System.out.println("异常，获取不到结果..");
                            continue;
                        }
                        printResult(reader, queryResultDto);
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

    private static void execSelectCommandGetResult(Integer databaseId, String sql, LineReader reader) {
        try (Socket socket = new Socket(ipAddress, port);
             // 获取输入流和输出流
             InputStream inputStream = socket.getInputStream();
             OutputStream outputStream = socket.getOutputStream();
             DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
             DataInputStream dataInputStream = new DataInputStream(inputStream)) {
            // 命令类型
            dataOutputStream.writeByte(CommandTypeConstant.DB_QUERY_PAGE);
            // 数据库id
            dataOutputStream.writeInt(databaseId);
            // SQL
            ReadWriteUtil.writeString(dataOutputStream, sql);
            // 获取结果
            Packet packet = TcpDataSender.readPacket(dataInputStream);
            if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
                OkPacket okPacket = (OkPacket) packet;
                QueryResultDto queryResultDto = (QueryResultDto) okPacket.getContent();
                // 如果数据量大于1000行，先大于前1000行
                int row = queryResultDto.getRows().length > PRINT_LIMIT ? PRINT_LIMIT : queryResultDto.getRows().length;
                PrintResultUtil.printTopRows(queryResultDto, row);
                // 按下Enter键, 逐行输出数据直到数据全部打印完成或者按q结束
                String input;
                if ((queryResultDto.getRows() != null && queryResultDto.getRows().length > PRINT_LIMIT) || queryResultDto.getHasNext() != (byte)0) {
                    int currIndex = PRINT_LIMIT;
                    String outputRowStr = PrintResultUtil.getOutputRowStr(queryResultDto, currIndex);

                    while ((input = reader.readLine(outputRowStr)) != null) {
                        // 输入是空，表示没有输入是直接按下enter
                        if (input.isEmpty()) {
                            currIndex++;
                            // 当前批次的数据已经都遍历完
                            if (currIndex >= queryResultDto.getRows().length) {
                                // 判断是否还有下一批次
                                if (queryResultDto.getHasNext() == (byte) 0) {
                                    break;
                                } else {
                                    // 获取下一批数据
                                    dataOutputStream.writeByte(1);
                                    // 获取结果
                                    packet = TcpDataSender.readPacket(dataInputStream);
                                    if (packet.getPacketType() == Packet.PACKET_TYPE_OK) {
                                        okPacket = (OkPacket) packet;
                                        queryResultDto = (QueryResultDto) okPacket.getContent();
                                        currIndex = 0;
                                    } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
                                        ErrPacket errPacket = (ErrPacket) packet;
                                        System.out.println("sql执行失败,错误码: " + errPacket.getErrCode() + "，错误信息: " + errPacket.getErrMsg());
                                    } else {
                                        System.out.println("不支持的packet type" + packet.getPacketType());
                                    }
                                }
                            }
                            outputRowStr = PrintResultUtil.getOutputRowStr(queryResultDto, currIndex);
                            continue;
                        }

                        if ("q".equalsIgnoreCase(input)) {
                            break;
                        }
                        outputRowStr = "";
                    }
                }

                if (queryResultDto.getDesc() != null) {
                    System.out.println(queryResultDto.getDesc());
                }
                System.out.println();


            } else if (packet.getPacketType() == Packet.PACKET_TYPE_ERR) {
                ErrPacket errPacket = (ErrPacket) packet;
                System.out.println("sql执行失败,错误码: " + errPacket.getErrCode() + "，错误信息: " + errPacket.getErrMsg());
            } else {
                System.out.println("不支持的packet type" + packet.getPacketType());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void printResult(LineReader reader, QueryResultDto queryResultDto) {
        String input;
        int dataLength = queryResultDto.getRows().length;
        int currIndex = PRINT_LIMIT;
        if (queryResultDto.getRows() != null && queryResultDto.getRows().length > PRINT_LIMIT) {
            PrintResultUtil.printTopRows(queryResultDto, PRINT_LIMIT);

            String outputRowStr = PrintResultUtil.getOutputRowStr(queryResultDto, currIndex);
            // 逐行输出数据直到按下Enter键
            while ((input = reader.readLine(outputRowStr)) != null) {
                if (input.isEmpty()) {
                    currIndex++;
                    if (currIndex >= dataLength) {
                        break;
                    }
                    outputRowStr = PrintResultUtil.getOutputRowStr(queryResultDto, currIndex);
                    continue;
                }

                if ("q".equalsIgnoreCase(input)) {
                    break;
                }
                outputRowStr = "";
            }

            if (queryResultDto.getDesc() != null) {
                System.out.println(queryResultDto.getDesc());
            }
            System.out.println();

        } else {
            PrintResultUtil.printResult(queryResultDto);
        }
    }


    private static boolean isShowDatabases(String[] inputWords) {
        return (inputWords.length >= 2 && ("SHOW".equals(inputWords[0].toUpperCase()) && "DATABASES".equals(inputWords[1].toUpperCase())));
    }

    private static boolean isCreateDatabase(String[] inputWords) {
        return (inputWords.length >= 2 && ("CREATE".equals(inputWords[0].toUpperCase()) && "DATABASE".equals(inputWords[1].toUpperCase())));
    }


    private static LineReader buildLineReader(Terminal terminal, DatabaseInfo database) {
        return LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(new TabCompleter(database))
                .build();
    }

    private static String[] getWords(String str) {
        List<String> workList = new ArrayList<>();
        str = str.trim();
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
                if (charArray[end] == ';') {
                    return workList.toArray(new String[0]);
                }
            }
            end++;
        }

        if (start < charArray.length) {
            String word = str.substring(start, charArray.length).trim();
            if (word.length() > 0) {
                workList.add(word);
            }
        }

        return workList.toArray(new String[0]);
    }


    private static void printDatabaseMsg() {
        System.out.println(
                "  __     __                    _____   ____   _\n" +
                        "  \\ \\   / /                   / ____| / __ \\ | |\n" +
                        "   \\ \\_/ /__ _  _ __   _   _ | (___  | |  | || |\n" +
                        "    \\   // _` || '_ \\ | | | | \\___ \\ | |  | || |\n" +
                        "     | || (_| || | | || |_| | ____) || |__| || |____\n" +
                        "     |_| \\__,_||_| |_| \\__, ||_____/  \\___\\_\\|______|\n" +
                        "                        __/ |  yanySQL 1.0\n" +
                        "                       |___/   author: 摸鱼大师");
        System.out.println("请输入命令...");
    }


}
