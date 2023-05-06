package com.moyu.test.store.metadata;

import com.moyu.test.constant.JavaTypeConstant;
import com.moyu.test.store.FileStore;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;
import com.moyu.test.util.DataUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class DatabaseMetadataStore {

    private String filePath;

    private String DATABASE_META_FILE_NAME = "database.meta";

    private FileStore fileStore;

    private List<DatabaseMetadata> databaseMetadataList = new ArrayList<>();



    public DatabaseMetadataStore(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }


    public void createDatabase(String databaseName) {
        synchronized (DatabaseMetadataStore.class) {
            checkDbName(databaseName);
            DatabaseMetadata metadata = null;
            DatabaseMetadata lastData = getLastData();

            int nextDatabaseId = 0;
            long startPos = 0L;
            if (lastData == null) {
                metadata = new DatabaseMetadata(databaseName, nextDatabaseId, startPos);
            } else {
                nextDatabaseId = lastData.getDatabaseId() + 1;
                startPos = lastData.getStartPos() + lastData.getTotalByteLen();
                metadata = new DatabaseMetadata(databaseName, nextDatabaseId, startPos);
            }

            ByteBuffer byteBuffer = metadata.getByteBuffer();
            fileStore.write(byteBuffer, startPos);
            databaseMetadataList.add(metadata);
        }
    }

    public List<DatabaseMetadata> getAllData(){
        return databaseMetadataList;
    }


    private void checkDbName(String databaseName) {
        for (DatabaseMetadata metadata : databaseMetadataList) {
            if (databaseName.equals(metadata.getName())) {
                throw new RuntimeException("数据库" + databaseName + "已存在");
            }
        }
    }

    private DatabaseMetadata getLastData() {
        if (databaseMetadataList.size() > 0) {
            return databaseMetadataList.get(databaseMetadataList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {
        String databasePath = filePath + File.separator + DATABASE_META_FILE_NAME;
        // 1、初始化"数据库"的元数据文件，不存在会创建文件，并把所有数据库信息读取到内存
        File dbFile = new File(databasePath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileStore = new FileStore(databasePath);
        long endPosition = fileStore.getEndPosition();
        if (endPosition > JavaTypeConstant.INT_LENGTH) {
            long currPos = 0;
            while (currPos < endPosition) {
                int dataByteLen = DataUtils.readInt(fileStore.read(currPos, JavaTypeConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileStore.read(currPos, dataByteLen);
                DatabaseMetadata dbMetadata = new DatabaseMetadata(readBuffer);
                databaseMetadataList.add(dbMetadata);
                currPos += dataByteLen;
            }
        }
    }


    public void close() {
        if (fileStore != null) {
            fileStore.close();
        }
    }



}
