package com.moyu.xmz.store.accessor;

import com.moyu.xmz.store.common.meta.DatabaseMetadata;
import com.moyu.xmz.common.constant.JavaTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.util.DataUtils;
import com.moyu.xmz.common.util.PathUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class DatabaseMetaFileAccessor {

    private static final String DEFAULT_META_PATH =  PathUtil.getMetaDirPath();

    private String filePath;

    public static final String DATABASE_META_FILE_NAME = "database.meta";

    private FileAccessor fileAccessor;

    private List<DatabaseMetadata> databaseMetadataList = new ArrayList<>();


    public DatabaseMetaFileAccessor() throws IOException {
        this(DEFAULT_META_PATH);
    }

    public DatabaseMetaFileAccessor(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }


    public DatabaseMetadata createDatabase(String databaseName) {
        synchronized (DatabaseMetaFileAccessor.class) {
            checkDbName(databaseName);
            DatabaseMetadata lastData = getLastData();
            int nextDatabaseId = lastData == null ? 0 : lastData.getDatabaseId() + 1;
            long startPos = lastData == null ? 0L : lastData.getStartPos() + lastData.getTotalByteLen();
            DatabaseMetadata metadata = new DatabaseMetadata(databaseName, nextDatabaseId, startPos);
            ByteBuffer byteBuffer = metadata.getByteBuffer();
            fileAccessor.write(byteBuffer, startPos);
            databaseMetadataList.add(metadata);
            return metadata;
        }
    }

    public void dropDatabase(String databaseName) {
        synchronized (DatabaseMetaFileAccessor.class) {
            Integer index = getDatabaseIndex(databaseName);
            if (index == null) {
                ExceptionUtil.throwSqlExecutionException("数据库{}不存在", databaseName);
            }
            DatabaseMetadata metadata = databaseMetadataList.get(index);
            long startPos = metadata.getStartPos();
            // 如果数据是最后一个，直接清除
            if (index == databaseMetadataList.size() - 1) {
                fileAccessor.truncate(startPos);
            } else {
                // 非最后一个，后面数据都往前挪
                int i = index + 1;
                long writeStartPos = startPos;
                do {
                    DatabaseMetadata meta = databaseMetadataList.get(i);
                    meta.setStartPos(writeStartPos);
                    fileAccessor.write(meta.getByteBuffer(), writeStartPos);
                    writeStartPos += meta.getTotalByteLen();
                    i++;
                } while (i < databaseMetadataList.size());

                // 内存列表移除
                databaseMetadataList.remove(index.intValue());
                DatabaseMetadata lastData = getLastData();
                // 清除后面多余的磁盘数据
                fileAccessor.truncate(lastData.getStartPos() + lastData.getTotalByteLen());
            }
        }
    }

    public Integer getDatabaseIndex(String databaseName) {
        int idx = 0;
        while (idx < databaseMetadataList.size()) {
            if (databaseName.equals(databaseMetadataList.get(idx).getName())) {
                return idx;
            }
            idx++;
        }
        return null;
    }


    public List<DatabaseMetadata> getAllData(){
        return databaseMetadataList;
    }

    public DatabaseMetadata getDatabase(String databaseName) {
        for (DatabaseMetadata metadata : databaseMetadataList) {
            if (databaseName.equals(metadata.getName())) {
                return metadata;
            }
        }
        return null;
    }


    private void checkDbName(String databaseName) {
        for (DatabaseMetadata metadata : databaseMetadataList) {
            if (databaseName.equals(metadata.getName())) {
                ExceptionUtil.throwSqlExecutionException("数据库{}已存在", databaseName);
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
        fileAccessor = new FileAccessor(databasePath);
        long endPosition = fileAccessor.getEndPosition();
        if (endPosition > JavaTypeConstant.INT_LENGTH) {
            long currPos = 0;
            while (currPos < endPosition) {
                int dataByteLen = DataUtils.readInt(fileAccessor.read(currPos, JavaTypeConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileAccessor.read(currPos, dataByteLen);
                DatabaseMetadata dbMetadata = new DatabaseMetadata(readBuffer);
                databaseMetadataList.add(dbMetadata);
                currPos += dataByteLen;
            }
        }
    }


    public void close() {
        if (fileAccessor != null) {
            fileAccessor.close();
        }
    }



}
