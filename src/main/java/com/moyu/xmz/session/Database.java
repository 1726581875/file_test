package com.moyu.xmz.session;

import com.moyu.xmz.command.ddl.ShowDatabasesCmd;
import com.moyu.xmz.command.ddl.ShowTablesCmd;
import com.moyu.xmz.store.common.meta.DatabaseMeta;
import com.moyu.xmz.store.common.meta.TableMeta;
import com.moyu.xmz.common.exception.DbException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xiaomingzhang
 * @date 2023/7/14
 */
public class Database {

    private final static Map<Integer, Database> databaseMap = new ConcurrentHashMap<>();

    private Integer databaseId;

    private String dbName;

    /**
     * tableName,tableObj
     */
    private Map<String, Table> tableMap = new ConcurrentHashMap<>();

    static {
        ShowDatabasesCmd showDbCommand = new ShowDatabasesCmd();
        showDbCommand.exec();
        List<DatabaseMeta> resultList = showDbCommand.getResultList();
        if (resultList != null) {
            for (DatabaseMeta metadata : resultList) {
                Database database = new Database(metadata.getDatabaseId(), metadata.getName());
                databaseMap.put(metadata.getDatabaseId(), database);
                ShowTablesCmd showTablesCmd = new ShowTablesCmd(metadata.getDatabaseId());
                showTablesCmd.exec();
                List<TableMeta> tableMetaList = showTablesCmd.getResultList();
                for (TableMeta tableMeta : tableMetaList) {
                    database.addTable(new Table(tableMeta));
                }
            }
        }
    }

    public Database(Integer databaseId, String dbName) {
        this.databaseId = databaseId;
        this.dbName = dbName;
    }

    public Integer getDatabaseId() {
        return databaseId;
    }

    public static Database getDatabase(Integer databaseId) {
        Database database = databaseMap.get(databaseId);
        if (database == null) {
            throw new DbException("数据库不存在，id:" + databaseId);
        }
        return database;
    }

    public static Database getDatabase(String databaseName) {
        for (Database database : databaseMap.values()) {
            if(database.getDbName().equals(databaseName)) {
                return database;
            }
        }
        throw new DbException("数据库不存在，库名:" + databaseName);
    }


    public static void putDatabase(Integer databaseId, Database database) {
        databaseMap.put(databaseId, database);
    }

    public static void removeDatabase(Integer databaseId) {
        databaseMap.remove(databaseId);
    }

    public Table getTable(String tableName) {
        Table table = tableMap.get(tableName);
        if(table == null) {
            throw new DbException("表不存在，表名:" + tableName);
        }
        return table;
    }

    public void addTable(Table table){
        tableMap.put(table.getTableName(), table);
    }

    public void removeTable(String tableName){
        tableMap.remove(tableName);
    }

    public String getDbName() {
        return dbName;
    }

    public Map<String, Table> getTableMap() {
        return tableMap;
    }
}
