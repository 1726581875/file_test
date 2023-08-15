package com.moyu.test.session;

import com.moyu.test.command.Command;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xiaomingzhang
 * @date 2023/7/14
 */
public class QueryCacheUtil {

    private final static int INIT_SIZE = 16;

    private static Map<Integer, Map<String, Command>> queryCacheMap = new ConcurrentHashMap<>(INIT_SIZE);

    public static Command getQueryCache(Integer databaseId, String sql) {
        Map<String, Command> commandMap = queryCacheMap.get(databaseId);
        if (commandMap == null) {
            return null;
        }

        return commandMap.get(sql);
    }


    public static void putQueryCache(Integer databaseId, String sql, Command command) {
        Map<String, Command> commandMap = queryCacheMap.get(databaseId);
        if (commandMap == null) {
            commandMap = new ConcurrentHashMap<>(INIT_SIZE);
        }
        commandMap.put(sql, command);
        queryCacheMap.put(databaseId, commandMap);
    }


    public static void clearQueryCache(Integer databaseId) {
        queryCacheMap.remove(databaseId);
    }


}
