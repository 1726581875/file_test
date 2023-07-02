package com.moyu.test.session;

import com.moyu.test.constant.CommonConstant;

import java.util.UUID;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class ConnectSession {

    private String sessionId;

    private String userName;

    private Integer databaseId;

    private int transactionId;

    /**
     * yyStore b+树叶子节点存储行
     * anStore 按顺序存储的
     */
    private String storeType = CommonConstant.STORE_TYPE_YU;


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

    public String getStoreType() {
        return storeType;
    }
}
