package com.moyu.test;

import com.moyu.test.command.Command;
import com.moyu.test.command.Parser;
import com.moyu.test.command.SqlParser;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.session.Database;
import com.moyu.test.store.metadata.DatabaseMetadataStore;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;

import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/5/19
 * 一个便以测试的假终端
 */
public class Terminal {


    /**
     * 运行前要配置resource/config.properties文件下数据目录路径和元数据路径
     * @param args
     */
    public static void main(String[] args) {

        Parser parser = getSqlParser(null);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("yanySQL>");
            String inputStr = scanner.nextLine();
            // 退出命令
            if ("exit;".equals(inputStr) || "exit".equals(inputStr)) {
                break;
            }

            if("".equals(inputStr.trim())){
                continue;
            }

            // 进入数据库
            if (inputStr.startsWith("use ")) {
                String dbName = inputStr.split(" ")[1];
                Database database = Database.getDatabase(dbName);
                if(database != null) {
                    parser = getSqlParser(database);
                    System.out.println("ok");
                } else {
                    System.out.println("数据库不存在");
                }
                continue;
            }

            try {
                Command command = parser.prepareCommand(inputStr.trim());
                String[] exec = command.exec();
                Arrays.asList(exec).forEach(System.out::println);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        System.out.println("结束");

    }


    private static Parser getSqlParser(Database database) {
        ConnectSession connectSession = new ConnectSession(database);
        return new SqlParser(connectSession);
    }




}
