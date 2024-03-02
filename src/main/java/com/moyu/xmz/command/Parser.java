package com.moyu.xmz.command;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public interface Parser {
    /**
     * 解析sql语句，把sql转换为具体逻辑操作的命令
     * @param sql
     * @return
     */
    Command prepareCommand(String sql);
}
