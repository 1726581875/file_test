package com.moyu.test.command.dml.sql;

import com.moyu.test.exception.DbException;
import com.moyu.test.store.metadata.obj.Column;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/6/12
 */
public class TableColumnInfo {

    private Map<String, Column> tableAliasColumnMap = new HashMap<>();

    private Map<String, Map<String, Column>> allTableColumnMap = new HashMap<>();


    public void setColumn(Column[] columns, String tableAlias) {
        for (Column c : columns) {
            tableAliasColumnMap.put(tableAlias + "." + c.getColumnName(), c);
            Map<String, Column> columnMap = allTableColumnMap.get(tableAlias);
            if(columnMap == null) {
                columnMap = new HashMap<>();
            }
            columnMap.put(c.getColumnName(), c);
            allTableColumnMap.put(tableAlias, columnMap);
        }
    }

    public Column getColumn(String columnName) {
        if (columnName.contains(".")) {
            Column column = tableAliasColumnMap.get(columnName);
            if (column == null) {
                throw new DbException("字段" + columnName + "不存在");
            }
            return column;
        } else {
            String tableAlias = null;
            Column column = null;
            for (String key : allTableColumnMap.keySet()) {
                Map<String, Column> columnMap = allTableColumnMap.get(key);
                Column c = columnMap.get(columnName);
                if(c != null) {
                    if(column == null) {
                        tableAlias = key;
                        column = c;
                    } else {
                        throw new DbException("表" + tableAlias + "和" + key +"同时存在字段" + columnName);
                    }
                }
            }

            if(column == null) {
                throw new DbException("字段" + columnName + "不存在");
            }

            return column;
        }
    }


}
