package com.moyu.test.config;

/**
 * @author xiaomingzhang
 * @date 2023/6/26
 */
public class CommonConfig {

    /**
     * 物化阈值
     */
    public static int MATERIALIZATION_THRESHOLD = 10000;
    /**
     * 小于4M的表会直接读取整个表到内存
     */
    public static int TABLE_IN_MEMORY_MAX_SIZE = 4096 * 1024;

    /**
     * 是否启用查询缓存
     */
    public static boolean IS_ENABLE_QUERY_CACHE = true;


}
