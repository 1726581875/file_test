package com.moyu.xmz.command;


/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public interface Command {

    /**
     * 执行命令
     * @return
     */
    QueryResult exec();

}
