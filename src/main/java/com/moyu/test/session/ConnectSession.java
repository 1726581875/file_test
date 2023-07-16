package com.moyu.test.session;

import com.moyu.test.command.Command;
import com.moyu.test.command.SqlParser;
import com.moyu.test.config.CommonConfig;

import java.util.UUID;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class ConnectSession {

    private Database database;

    private String sessionId;

    private String userName;

    private Integer databaseId;

    private int transactionId;


    public Command prepareCommand(String sql) {
        if (CommonConfig.IS_ENABLE_QUERY_CACHE) {
            Command cacheCommand = QueryCacheUtil.getQueryCache(databaseId, sql);
            if (cacheCommand != null) {
                System.out.println("use query cache");
                cacheCommand.reUse();
                return cacheCommand;
            }

        }
        SqlParser sqlParser = new SqlParser(this);
        Command command = sqlParser.prepareCommand(sql);

        if (CommonConfig.IS_ENABLE_QUERY_CACHE) {
            QueryCacheUtil.putQueryCache(databaseId, sql, command);
        }

        return command;
    }


    public ConnectSession(Database database){
        this.databaseId = database.getDatabaseId();
        this.database = database;
        this.sessionId = UUID.randomUUID().toString().replace("-", "");
    }


    public ConnectSession(String userName, Integer databaseId) {
        this.userName = userName;
        this.databaseId = databaseId;
        this.sessionId = UUID.randomUUID().toString().replace("-", "");
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Integer databaseId) {
        this.databaseId = databaseId;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public Database getDatabase() {
        return database;
    }
}
