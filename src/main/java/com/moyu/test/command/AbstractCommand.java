package com.moyu.test.command;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public abstract class AbstractCommand implements Command {


    @Override
    public String[] exec() {
        String result = execute();
        String[] strings = new String[1];
        strings[0] = result;
        return strings;
    }

    @Override
    public String execCommand() {
        String[] exec = exec();
        return exec[0];
    }

    /**
     * 执行命令
     *
     * @param
     * @return
     */
    abstract public String execute();

}
