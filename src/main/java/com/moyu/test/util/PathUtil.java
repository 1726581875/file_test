package com.moyu.test.util;

import java.io.File;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class PathUtil {

    private static final String baseDir = "yanyu.database.basePath";

    private static final String metaDir = "yanyu.database.metaPath";

    private static final String logDir = "yanyu.database.logPath";


    public static String getBaseDirPath() {
        return PropertiesReadUtil.get(baseDir);
    }

    public static String getMetaDirPath() {
        return PropertiesReadUtil.get(metaDir);
    }

    public static String getLogDirPath() {
        return PropertiesReadUtil.get(logDir);
    }

    public static String getDataFilePath(Integer databaseId, String tableName) {
        String dirPath = getBaseDirPath() + File.separator + databaseId;
        FileUtil.createDirIfNotExists(dirPath);
        return dirPath + File.separator + tableName + ".dyu";
    }

    public static String getYanEngineDataFilePath(Integer databaseId, String tableName) {
        String dirPath = getBaseDirPath() + File.separator + databaseId;
        FileUtil.createDirIfNotExists(dirPath);
        return dirPath + File.separator + tableName + ".dyan";
    }


    public static String getIndexMetaPath(Integer databaseId) {
        return  getMetaDirPath() + File.separator + databaseId;
    }


    public static String getIndexFilePath(Integer databaseId, String tableName, String indexName) {
        String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
        String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
        return indexPath;
    }


}
