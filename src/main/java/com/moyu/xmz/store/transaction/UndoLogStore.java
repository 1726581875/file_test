package com.moyu.xmz.store.transaction;

import com.moyu.xmz.store.accessor.FileAccessor;
import com.moyu.xmz.common.util.DataUtils;
import com.moyu.xmz.common.util.PathUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/6/13
 */
public class UndoLogStore {

    private static final String DEFAULT_META_PATH =  PathUtil.getMetaDirPath();

    private String filePath;

    public static final String UNDO_LOG_FILE_NAME = "undo.log";

    private FileAccessor fileAccessor;

    private int transactionNum;

    private int maxTransactionId;

    private List<Transaction> transactionList = new ArrayList<>();

    private Map<Integer, Transaction> transactionMap = new HashMap<>();


    public UndoLogStore(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }

    private void init() throws IOException {
        String databasePath = filePath + File.separator + UNDO_LOG_FILE_NAME;
        File dbFile = new File(databasePath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        try {
            fileAccessor = new FileAccessor(databasePath);
            long endPosition = fileAccessor.getEndPosition();
            if (endPosition >= Transaction.BLOCK_SIZE) {
                this.transactionNum = DataUtils.readInt(fileAccessor.read(0, 4));
                this.maxTransactionId = DataUtils.readInt(fileAccessor.read(4, 8));
            } else {
                this.transactionNum = 0;
                this.maxTransactionId = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
            fileAccessor.close();
        }

    }


    public void saveTransaction(Transaction transaction) {
        transaction.setStartPos(transaction.getTransactionId() * Transaction.BLOCK_SIZE);
        this.fileAccessor.write(transaction.getByteBuffer(), transaction.getStartPos());
        updateMaxTransactionId();
    }


    public List<Transaction> getTransactionList() {
        if (this.transactionList == null) {
            this.transactionList = new ArrayList<>();
            long endPosition = fileAccessor.getEndPosition();
            long currPos = Transaction.BLOCK_SIZE;
            while (endPosition >= currPos + Transaction.BLOCK_SIZE) {
                ByteBuffer buffer = fileAccessor.read(currPos, Transaction.BLOCK_SIZE);
                Transaction transaction = new Transaction(buffer);
                this.transactionList.add(transaction);
                currPos += Transaction.BLOCK_SIZE;
            }
            return this.transactionList;
        } else {
            return this.transactionList;
        }
    }


    public Transaction getTransaction(int tid) {
        this.transactionList = new ArrayList<>();
        long endPosition = fileAccessor.getEndPosition();
        long currPos = Transaction.BLOCK_SIZE;
        while (endPosition >= currPos + Transaction.BLOCK_SIZE) {
            ByteBuffer buffer = fileAccessor.read(currPos, Transaction.BLOCK_SIZE);
            Transaction transaction = new Transaction(buffer);
            if(transaction.getTransactionId() == tid) {
                return transaction;
            }
            currPos += Transaction.BLOCK_SIZE;
        }
        return null;
    }



    public int getNextTransactionId() {
        synchronized (UndoLogStore.class) {
            this.maxTransactionId++;
            return this.maxTransactionId;
        }
    }


    private void updateMaxTransactionId() {
        synchronized (UndoLogStore.class) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
            DataUtils.writeInt(byteBuffer, this.maxTransactionId);
            byteBuffer.rewind();
            fileAccessor.write(byteBuffer, 0);
        }
    }



    public void close() {
        if (fileAccessor != null) {
            fileAccessor.close();
        }
    }



}
