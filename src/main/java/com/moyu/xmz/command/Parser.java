package com.moyu.xmz.command;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public interface Parser {
    Command prepareCommand(String sql);
}
