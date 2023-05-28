package com.moyu.test;

import com.moyu.test.command.Command;
import com.moyu.test.command.Parser;
import com.moyu.test.command.SqlParser;
import com.moyu.test.session.ConnectSession;
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

        // 初始化，查询所有数据库
        Map<String, Integer> databaseMap = getDatabaseMap();

        Parser parser = getSqlParser("xmz", 0);
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
                Integer databaseId = databaseMap.get(dbName.trim());
                if(databaseId != null) {
                    parser = getSqlParser("xmz", databaseId);
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


            // 重新更新数据库信息
            if(inputStr.startsWith("create database ") || inputStr.startsWith("drop database ")) {
                databaseMap = getDatabaseMap();
            }

        }

        System.out.println("结束");

    }


    private static Parser getSqlParser(String user, Integer databaseId) {
        ConnectSession connectSession = new ConnectSession(user, databaseId);
        return new SqlParser(connectSession);
    }

    private static Map<String, Integer> getDatabaseMap() {
        Map<String, Integer> databaseMap = new HashMap<>();
        DatabaseMetadataStore databaseStore = null;
        try {
            databaseStore = new DatabaseMetadataStore();
            List<DatabaseMetadata> allData = databaseStore.getAllData();
            for (DatabaseMetadata db : allData) {
                databaseMap.put(db.getName(), db.getDatabaseId());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            databaseStore.close();
        }
        return databaseMap;
    }




}
