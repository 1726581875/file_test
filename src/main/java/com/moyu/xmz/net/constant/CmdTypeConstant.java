package com.moyu.xmz.net.constant;

/**
 * @author xiaomingzhang
 * @date 2023/9/10
 */
public class CmdTypeConstant {
    /**
     * 查询数据库信息
     */
    public static final byte DB_INFO = 0;
    /**
     * 查询命令（返回数据对象）
     */
    public static final byte DB_QUERY = 1;
    /**
     * 查询命令，响应结果格式化字符串（提供给远程终端程序打印输出）
     */
    public static final byte DB_QUERY_RES_STR = 2;

    /**
     * 预编译语句的查询
     */
    public static final byte DB_PREPARED_QUERY = 3;

    /**
     * 查询命令,分页返回
     */
    public static final byte DB_QUERY_PAGE = 4;

}
