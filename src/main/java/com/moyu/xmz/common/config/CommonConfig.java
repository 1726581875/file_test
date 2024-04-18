package com.moyu.xmz.common.config;

/**
 * @author xiaomingzhang
 * @date 2023/6/26
 */
public class CommonConfig {

    /**
     * 物化阈值
     */
    public static final int MATERIALIZATION_THRESHOLD = 10000;
    /**
     * 小于4M的表会直接读取整个表到内存
     */
    public static final int TABLE_IN_MEMORY_MAX_SIZE = 4096 * 1024;

    /**
     * 是否启用查询缓存
     */
    public static final boolean IS_ENABLE_QUERY_CACHE = true;

    /**
     * 查询命令，一次网络传输最大行数
     */
    public static final int NET_TRAN_MAX_ROW_SIZE = 10000;


    /**
     * 最大内存排序的行记录大小，超出转为磁盘排序
     */
    public static final int MAX_MEMORY_SORT_ROW_SIZE = 100000;

    /**
     * 函数嵌套的最大层数
     */
    public static final int FUNCTION_MAX_DEPTH = 30;


    private CommonConfig(){}


}
