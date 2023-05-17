package com.moyu.test.util;

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


}
