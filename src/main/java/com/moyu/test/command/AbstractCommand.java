package com.moyu.test.command;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 * TODO 统一命令
 */
public abstract class AbstractCommand implements Command {


    /**
     * 执行命令
     * @param
     * @return
     */
    abstract public String execute();

}
