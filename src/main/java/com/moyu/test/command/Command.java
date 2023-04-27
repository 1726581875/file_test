package com.moyu.test.command;

import com.moyu.test.store.Chunk;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/4/27
 */
public interface Command {
    List<Chunk> execute();
}
