package com.moyu.xmz.store.transaction;

import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.common.util.PathUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class TransactionManager {

    private static int nextId = 1;

    private static Map<Integer,Transaction> transactionMap = new HashMap<>();

    public static int initTransaction(ConnectSession session) {
        Transaction transaction = null;
        UndoLogStore undoLogStore = null;
        try {
            undoLogStore = new UndoLogStore(PathUtils.getLogDirPath());
            int transactionId = undoLogStore.getNextTransactionId();
            long startTime = System.currentTimeMillis();
            transaction = new Transaction(transactionId, Transaction.STATUS_ACTIVITY, startTime);
            undoLogStore.saveTransaction(transaction);
            session.setTransactionId(transactionId);
            return transactionId;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            undoLogStore.close();
        }
        return -1;
    }

    public static Transaction recordTransaction(Transaction transaction) {
        UndoLogStore undoLogStore = null;
        try {
            undoLogStore = new UndoLogStore(PathUtils.getLogDirPath());
            undoLogStore.saveTransaction(transaction);
            return transaction;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            undoLogStore.close();
        }
        return transaction;
    }



    public static Transaction getTransaction(int transactionId) {
        if (transactionId <= 0) {
            return null;
        }

        UndoLogStore undoLogStore = null;
        try {
            undoLogStore = new UndoLogStore(PathUtils.getLogDirPath());
            Transaction transaction = undoLogStore.getTransaction(transactionId);
            if (transaction == null) {
                throw new DbException("获取不到事务信息");
            }
            return transaction;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            undoLogStore.close();
        }
        throw new DbException("获取不到事务信息");
    }





}
