package com.moyu.test.session;

import com.moyu.test.command.ddl.ShowDatabasesCommand;
import com.moyu.test.command.ddl.ShowTablesCommand;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.metadata.obj.DatabaseMetadata;
import com.moyu.test.store.metadata.obj.TableMetadata;

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
        ShowDatabasesCommand showDatabasesCommand = new ShowDatabasesCommand();
        showDatabasesCommand.execAndGetResult();
        List<DatabaseMetadata> resultList = showDatabasesCommand.getResultList();
        if (resultList != null) {
            for (DatabaseMetadata metadata : resultList) {
                Database database = new Database(metadata.getDatabaseId(), metadata.getName());
                databaseMap.put(metadata.getDatabaseId(), database);
                ShowTablesCommand showTablesCommand = new ShowTablesCommand(metadata.getDatabaseId());
                showTablesCommand.execute();
                List<TableMetadata> tableMetadataList = showTablesCommand.getResultList();
                for (TableMetadata tableMetadata : tableMetadataList) {
                    database.addTable(new Table(tableMetadata));
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
        throw new DbException("数据库不存在，name:" + databaseName);
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
