package com.moyu.xmz.common.util;

import com.moyu.xmz.common.config.PropertiesConfigLoader;

import java.io.File;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class PathUtils {

    private static final String baseDir = "yanyu.database.basePath";

    private static final String metaDir = "yanyu.database.metaPath";

    private static final String logDir = "yanyu.database.logPath";


    public static String getBaseDirPath() {
        return PropertiesConfigLoader.get(baseDir);
    }

    public static String getMetaDirPath() {
        return PropertiesConfigLoader.get(metaDir);
    }

    public static String getLogDirPath() {
        return PropertiesConfigLoader.get(logDir);
    }

    public static String getDataFilePath(Integer databaseId, String tableName) {
        String dirPath = getBaseDirPath() + File.separator + databaseId;
        FileUtils.createDirIfNotExists(dirPath);
        return dirPath + File.separator + tableName + ".dyu";
    }

    public static String getYanEngineDataFilePath(Integer databaseId, String tableName) {
        String dirPath = getBaseDirPath() + File.separator + databaseId;
        FileUtils.createDirIfNotExists(dirPath);
        return dirPath + File.separator + tableName + ".dyan";
    }


    public static String getIndexMetaPath(Integer databaseId) {
        return  getMetaDirPath() + File.separator + databaseId;
    }


    public static String getIndexFilePath(Integer databaseId, String tableName, String indexName) {
        String dirPath = PathUtils.getBaseDirPath() + File.separator + databaseId;
        String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
        return indexPath;
    }


}
