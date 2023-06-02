package com.moyu.test.util;

import java.io.File;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class PathUtil {

    private static final String baseDir = "yanyu.database.basePath";

    private static final String metaDir = "yanyu.database.metaPath";


    public static String getBaseDirPath() {
        return PropertiesReadUtil.get(baseDir);
    }

    public static String getMetaDirPath() {
        return PropertiesReadUtil.get(metaDir);
    }

    public static String getDataFilePath(Integer databaseId, String tableName) {
        String dirPath = getBaseDirPath() + File.separator + databaseId;
        FileUtil.createDirIfNotExists(dirPath);
        return dirPath + File.separator + tableName + ".d";
    }

    public static String getIndexFilePath(Integer databaseId, String tableName, String indexName) {
        String dirPath = PathUtil.getBaseDirPath() + File.separator + databaseId;
        String indexPath = dirPath + File.separator + tableName + "_" + indexName + ".idx";
        return indexPath;
    }


}
