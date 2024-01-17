package com.moyu.xmz.command.dml.sql;

import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.common.dto.Column;

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
        // 字段包含”.“, 如 t.id这种可以直接配置tableAliasColumnMap
        if (columnName.contains(".")) {
            Column column = tableAliasColumnMap.get(columnName);
            if (column == null) {
                ExceptionUtil.throwDbException("字段{}不存在", columnName);
            }
            return column;
        }
        // 不带表别名的情况
        String tableAlias = null;
        Column column = null;
        // 遍历所有表，逐一匹配字段
        for (Map.Entry<String, Map<String, Column>> entry : allTableColumnMap.entrySet()) {
            Map<String, Column> columnMap = entry.getValue();
            Column c = columnMap.get(columnName);
            if (c != null) {
                if (column == null) {
                    tableAlias = entry.getKey();
                    column = c;
                } else {
                    ExceptionUtil.throwDbException("表{}和{}同时存在字段{}", tableAlias, entry.getKey(), columnName);
                }
            }
        }

        if (column == null) {
            ExceptionUtil.throwDbException("字段{}不存在", columnName);
        }

        return column;

    }


}
