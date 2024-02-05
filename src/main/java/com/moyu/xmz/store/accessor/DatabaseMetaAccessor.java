package com.moyu.xmz.store.accessor;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.common.util.FileUtils;
import com.moyu.xmz.store.common.meta.DatabaseMeta;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.common.util.PathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public class DatabaseMetaAccessor {

    private static final String DEFAULT_META_PATH =  PathUtils.getMetaDirPath();

    private String filePath;

    public static final String DATABASE_META_FILE_NAME = "database.meta";

    private FileAccessor fileAccessor;

    private List<DatabaseMeta> databaseMetaList = new ArrayList<>();


    public DatabaseMetaAccessor() throws IOException {
        this(DEFAULT_META_PATH);
    }

    public DatabaseMetaAccessor(String filePath) throws IOException {
        this.filePath = filePath;
        init();
    }


    public DatabaseMeta createDatabase(String databaseName) {
        synchronized (DatabaseMetaAccessor.class) {
            checkDbName(databaseName);
            DatabaseMeta lastData = getLastData();
            int nextDatabaseId = lastData == null ? 0 : lastData.getDatabaseId() + 1;
            long startPos = lastData == null ? 0L : lastData.getStartPos() + lastData.getTotalByteLen();
            DatabaseMeta metadata = new DatabaseMeta(databaseName, nextDatabaseId, startPos);
            ByteBuffer byteBuffer = metadata.getByteBuffer();
            fileAccessor.write(byteBuffer, startPos);
            databaseMetaList.add(metadata);
            return metadata;
        }
    }

    public void dropDatabase(String databaseName) {
        synchronized (DatabaseMetaAccessor.class) {
            Integer index = getDatabaseIndex(databaseName);
            if (index == null) {
                ExceptionUtil.throwSqlExecutionException("数据库{}不存在", databaseName);
            }
            DatabaseMeta metadata = databaseMetaList.get(index);
            long startPos = metadata.getStartPos();
            // 如果数据是最后一个，直接清除
            if (index == databaseMetaList.size() - 1) {
                fileAccessor.truncate(startPos);
            } else {
                // 非最后一个，后面数据都往前挪
                int i = index + 1;
                long writeStartPos = startPos;
                do {
                    DatabaseMeta meta = databaseMetaList.get(i);
                    meta.setStartPos(writeStartPos);
                    fileAccessor.write(meta.getByteBuffer(), writeStartPos);
                    writeStartPos += meta.getTotalByteLen();
                    i++;
                } while (i < databaseMetaList.size());

                // 内存列表移除
                databaseMetaList.remove(index.intValue());
                DatabaseMeta lastData = getLastData();
                // 清除后面多余的磁盘数据
                fileAccessor.truncate(lastData.getStartPos() + lastData.getTotalByteLen());
            }
        }
    }

    public Integer getDatabaseIndex(String databaseName) {
        int idx = 0;
        while (idx < databaseMetaList.size()) {
            if (databaseName.equals(databaseMetaList.get(idx).getName())) {
                return idx;
            }
            idx++;
        }
        return null;
    }


    public List<DatabaseMeta> getAllData(){
        return databaseMetaList;
    }

    public DatabaseMeta getDatabase(String databaseName) {
        for (DatabaseMeta metadata : databaseMetaList) {
            if (databaseName.equals(metadata.getName())) {
                return metadata;
            }
        }
        return null;
    }


    private void checkDbName(String databaseName) {
        for (DatabaseMeta metadata : databaseMetaList) {
            if (databaseName.equals(metadata.getName())) {
                ExceptionUtil.throwSqlExecutionException("数据库{}已存在", databaseName);
            }
        }
    }

    private DatabaseMeta getLastData() {
        if (databaseMetaList.size() > 0) {
            return databaseMetaList.get(databaseMetaList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {

        FileUtils.createDirIfNotExists(filePath);

        String databasePath = filePath + File.separator + DATABASE_META_FILE_NAME;
        // 初始化"数据库"的元数据文件，不存在则创建文件，并把所有数据库信息读取到内存
        File dbFile = new File(databasePath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileAccessor = new FileAccessor(databasePath);
        long endPosition = fileAccessor.getEndPosition();
        if (endPosition > CommonConstant.INT_LENGTH) {
            long currPos = 0;
            while (currPos < endPosition) {
                int dataByteLen = DataByteUtils.readInt(fileAccessor.read(currPos, CommonConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileAccessor.read(currPos, dataByteLen);
                DatabaseMeta dbMetadata = new DatabaseMeta(readBuffer);
                databaseMetaList.add(dbMetadata);
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
