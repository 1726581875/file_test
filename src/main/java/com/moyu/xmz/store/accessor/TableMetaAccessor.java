package com.moyu.xmz.store.accessor;

import com.moyu.xmz.common.constant.CommonConstant;
import com.moyu.xmz.store.common.block.TableColumnBlock;
import com.moyu.xmz.store.common.meta.ColumnMeta;
import com.moyu.xmz.store.common.meta.TableMeta;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.common.util.DataByteUtils;
import com.moyu.xmz.common.util.FileUtils;
import com.moyu.xmz.common.util.PathUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/8
 */
public class TableMetaAccessor {


    private static final String DEFAULT_META_PATH =  PathUtils.getMetaDirPath();

    private String filePath;

    public static final String TABLE_META_FILE_NAME = "table.meta";

    private FileAccessor fileAccessor;

    private Integer databaseId;

    private List<TableMeta> tableMetaList = new ArrayList<>();


    public TableMetaAccessor(Integer databaseId) throws IOException {
        this(databaseId, DEFAULT_META_PATH);
    }


    public TableMetaAccessor(Integer databaseId, String filePath) throws IOException {
        this.filePath = filePath;
        this.databaseId = databaseId;
        init();
    }


    public TableMeta createTable(String tableName, String engineType) {
        TableMeta metadata = null;
        synchronized (TableMetaAccessor.class) {
            checkTableName(tableName);
            TableMeta lastData = getLastTable();
            int nextTableId = lastData == null ? 0 : lastData.getTableId() + 1;
            long startPos = lastData == null ? 0L : lastData.getStartPos() + lastData.getTotalByteLen();
            metadata = new TableMeta(tableName, nextTableId, databaseId, startPos, null);
            metadata.setEngineType(engineType);
            ByteBuffer byteBuffer = metadata.getByteBuffer();
            fileAccessor.write(byteBuffer, startPos);
            tableMetaList.add(metadata);
        }
        return metadata;
    }


    public TableMeta dropTable(String tableName) {
        TableMeta tableMeta = null;
        int dropIndex = 0;
        for (int i = 0; i < tableMetaList.size(); i++) {
            TableMeta metadata = tableMetaList.get(i);
            if (tableName.equals(metadata.getTableName())) {
                tableMeta = metadata;
                dropIndex = i;
                break;
            }
        }

        if(tableMeta == null) {
            ExceptionUtil.throwSqlExecutionException("表{}不存在", tableName);
        }

        long startPos = tableMeta.getStartPos();
        long endPos = tableMeta.getStartPos() + tableMeta.getTotalByteLen();
        if(endPos >= fileAccessor.getEndPosition()) {
            fileAccessor.truncate(startPos);
        } else {
            long nextStartPos = endPos;
            while (nextStartPos < fileAccessor.getEndPosition()) {
                int dataByteLen = DataByteUtils.readInt(fileAccessor.read(nextStartPos, CommonConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileAccessor.read(nextStartPos, dataByteLen);
                TableMeta metadata = new TableMeta(readBuffer);
                metadata.setStartPos(startPos);
                fileAccessor.write(metadata.getByteBuffer(), startPos);
                startPos += dataByteLen;
                nextStartPos += dataByteLen;
            }
            fileAccessor.truncate(startPos);
        }

        tableMetaList.remove(dropIndex);

        try {
            init();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return tableMeta;
    }



    public List<TableMeta> getCurrDbAllTable() {
        List<TableMeta> currDbTables = new ArrayList<>();
        for (TableMeta table : tableMetaList) {
            if(databaseId.equals(table.getDatabaseId())) {
                currDbTables.add(table);
            }
        }
        return currDbTables;
    }

    public TableMeta getTable(String tableName) {
        for (TableMeta metadata : tableMetaList) {
            if(databaseId.equals(metadata.getDatabaseId())
                    && metadata.getTableName().equals(tableName)) {
                return metadata;
            }
        }
        return null;
    }



    public List<ColumnMeta> getColumnList(Integer tableId) {
        ColumnMetaAccessor columnMetaAccessor = null;
        try {
            columnMetaAccessor = new ColumnMetaAccessor(filePath);
            TableColumnBlock columnBlock = columnMetaAccessor.getColumnMap().get(tableId);
            if (columnBlock != null) {
                return columnBlock.getColumnMetaList();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (columnMetaAccessor != null) {
                columnMetaAccessor.close();
            }
        }
        return new ArrayList<>();
    }


    private void checkTableName(String tableName) {
        for (TableMeta metadata : tableMetaList) {
            if (tableName.equals(metadata.getTableName())
                    && databaseId.equals(metadata.getDatabaseId())) {
                try {
                    init();
                    for (TableMeta m : tableMetaList) {
                        if (tableName.equals(m.getTableName()) && databaseId.equals(m.getDatabaseId())) {
                            System.out.println("表" + m.getTableName() + "已存在");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                throw new SqlExecutionException("表" + tableName + "已存在");
            }
        }
    }

    private TableMeta getLastTable() {
        if (tableMetaList.size() > 0) {
            return tableMetaList.get(tableMetaList.size() - 1);
        } else {
            return null;
        }
    }


    private void init() throws IOException {
        // 初始化table的元数据文件，不存在会创建文件，并把所有表信息读取到内存
        String basePath = filePath + File.separator + databaseId;
        FileUtils.createDirIfNotExists(basePath);

        String tableMetaPath = basePath + File.separator + TABLE_META_FILE_NAME;

        File dbFile = new File(tableMetaPath);
        if (!dbFile.exists()) {
            dbFile.createNewFile();
        }
        fileAccessor = new FileAccessor(tableMetaPath);
        long endPosition = fileAccessor.getEndPosition();
        tableMetaList = new ArrayList<>();
        if (endPosition > CommonConstant.INT_LENGTH) {
            long currPos = 0;
            while (currPos < endPosition) {
                int dataByteLen = DataByteUtils.readInt(fileAccessor.read(currPos, CommonConstant.INT_LENGTH));
                ByteBuffer readBuffer = fileAccessor.read(currPos, dataByteLen);
                TableMeta dbMetadata = new TableMeta(readBuffer);
                tableMetaList.add(dbMetadata);
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
