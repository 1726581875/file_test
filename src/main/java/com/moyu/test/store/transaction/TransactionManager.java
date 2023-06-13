package com.moyu.test.store.transaction;

import com.moyu.test.session.ConnectSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class TransactionManager {

    private static int nextId = 1;

    private static Map<Integer,Transaction> transactionMap = new HashMap<>();

    public static Transaction initTransaction(ConnectSession session) {
        int transactionId = nextId;
        nextId++;
        long startTime = System.currentTimeMillis();
        Transaction transaction = new Transaction(transactionId,Transaction.STATUS_ACTIVITY, startTime);
        transactionMap.put(transactionId, transaction);
        session.setTransactionId(transactionId);
        return transaction;
    }


    public static Transaction getTransaction(int transactionId) {
        if(transactionId <= 0) {
            return null;
        }
        return transactionMap.get(transactionId);
    }





}
