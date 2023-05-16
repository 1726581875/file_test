package com.moyu.test.command;

import com.moyu.test.command.ddl.*;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.obj.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class SqlParser implements Parser {

    private ConnectSession connectSession;


    /**
     * 转换大写后sql
     */
    private String upperCaseSql;
    /**
     * 原sql
     */
    private String originalSql;
    /**
     * 转换大写后sql字符数组
     */
    private char[] sqlCharArr;

    private int currStartIndex;

    private int cursorIndex;


    private static final String CREATE = "CREATE";
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";
    private static final String SELECT = "SELECT";

    private static final String DROP = "DROP";
    private static final String SHOW = "SHOW";
    private static final String DESC = "DESC";

    private static final String DATABASE = "DATABASE";
    private static final String TABLE = "TABLE";


    public SqlParser(ConnectSession connectSession) {
        this.connectSession = connectSession;
    }

    @Override
    public Command prepareCommand(String sql) {
        this.originalSql = sql;
        this.upperCaseSql = sql.toUpperCase();
        this.sqlCharArr = this.upperCaseSql.toCharArray();
        this.currStartIndex = 0;
        this.cursorIndex = 0;

        String firstKeyWord = getNextKeyWord();

        switch (firstKeyWord) {
            case CREATE:
                skipSpace();
                String nextKeyWord = getNextKeyWord();
                switch (nextKeyWord) {
                    // create database
                    case DATABASE:
                        skipSpace();
                        String databaseName = getNextKeyWord();
                        CreateDatabaseCommand command = new CreateDatabaseCommand();
                        command.setDatabaseName(databaseName);
                        return command;
                        // create table
                    case TABLE:
                        return getCreateTableCommand();
                    default:
                        throw new SqlIllegalException("sql语法有误");
                }
            case UPDATE:
                break;
            case DELETE:
                break;
            case SELECT:
                break;
            case DROP:
                skipSpace();
                String keyWord = getNextKeyWord();
                switch (keyWord) {
                    // drop database
                    case DATABASE:
                        skipSpace();
                        String databaseName = getNextOriginalWord();
                        DropDatabaseCommand command = new DropDatabaseCommand();
                        command.setDatabaseName(databaseName);
                        return command;
                        // drop table
                    case TABLE:
                        skipSpace();
                        String tableName = getNextOriginalWord();
                        return new DropTableCommand(this.connectSession.getDatabaseId(), tableName);
                    default:
                        throw new SqlIllegalException("sql语法有误");
                }
            case SHOW:
                skipSpace();
                String word11 = getNextKeyWord();
                // show databases
                if ("DATABASES".equals(word11)) {
                    return new ShowDatabasesCommand();
                    // show tables
                } else if ("TABLES".equals(word11)) {
                    return new ShowTablesCommand(this.connectSession.getDatabaseId());
                }
                break;
            case DESC:
                skipSpace();
                String word12 = getNextOriginalWord();
                return new DescTableCommand(this.connectSession.getDatabaseId(), word12);
            default:
                throw new SqlIllegalException("sql语法有误");
        }
        return null;
    }


    private CreateTableCommand getCreateTableCommand() {
        // 解析tableName
        String tableName = null;
        skipSpace();
        int i = currStartIndex;
        while (true) {
            if(i >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[i] == ' ' || sqlCharArr[i] == '(') {
                tableName = originalSql.substring(currStartIndex, i);
                currStartIndex = i;
                break;
            }
            i++;
        }

        // 解析columns
        skipSpace();
        List<Column> columnList = new ArrayList<>();
        int idx = currStartIndex;
        while (true) {
            if(idx >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            // create table (
            // 解析"("里面内容
            if(sqlCharArr[idx] == '(') {
                idx++;
                currStartIndex++;
                int columnIndex = 0;
                boolean columnNotOpen = true;
                while (true) {
                    skipSpace();
                    if (idx >= sqlCharArr.length) {
                        break;
                    }

                    if ((sqlCharArr[idx] == ')' && columnNotOpen)) {
                        Column column = parseColumn(columnIndex, originalSql.substring(currStartIndex, idx));
                        columnList.add(column);
                        columnIndex++;
                        break;
                    }

                    if (sqlCharArr[idx] == '(' && columnNotOpen) {
                        columnNotOpen = false;
                    }
                    if (sqlCharArr[idx] == ')' && !columnNotOpen) {
                        columnNotOpen = true;
                    }

                    if (sqlCharArr[idx] == ',') {
                        Column column = parseColumn(columnIndex, originalSql.substring(currStartIndex, idx));
                        columnList.add(column);
                        columnIndex++;
                        currStartIndex = idx + 1;
                    }
                    idx++;
                }
            }

            if(sqlCharArr[idx] == ')') {
                break;
            }
            idx++;
            currStartIndex++;
        }

        // 构造创建表命令
        CreateTableCommand command = new CreateTableCommand();
        command.setDatabaseId(connectSession.getDatabaseId());
        command.setTableName(tableName);
        command.setColumnList(columnList);
        return command;
    }


    /**
     *
     * @param columnIndex
     * @param columnStr
     * @return
     */
    private Column parseColumn(int columnIndex, String columnStr) {

        String[] columnKeyWord = columnStr.trim().split(" ");
        if(columnKeyWord.length < 2) {
            throw new SqlIllegalException("sql语法异常," + columnStr);
        }

        // 解析字段
        String columnName = columnKeyWord[0];
        if((columnName.startsWith("`") && columnName.endsWith("`"))
                || (columnName.startsWith("\"") && columnName.endsWith("\""))) {
            columnName = columnStr.substring(1, columnName.length() - 1);
        }

        // 解析字段类型、字段长度。示例:varchar(64)、int
        String column = columnKeyWord[1];

        int columnLength = -1;
        // 括号开始
        int start = -1;
        // 括号结束
        int end = -1;
        for (int i = 0; i < column.length(); i++) {
            if(column.charAt(i) == '('){
                start = i;
            }
            if(column.charAt(i) == ')'){
                end = i;
            }
        }

        String typeName = null;
        if(end > start && start > 0) {
            columnLength = Integer.valueOf(column.substring(start + 1, end));
            typeName = column.substring(0, start);
        } else {
            typeName = column;
        }
        Byte columnType = ColumnTypeEnum.getColumnTypeByName(typeName);
        if(columnType == null) {
            throw new SqlIllegalException("不支持类型:" + typeName);
        }

        // 如果没有指定字段长度像int、bigint等，要自动给长度
        if(columnLength == -1) {
            if(DbColumnTypeConstant.INT_4 == columnType) {
                columnLength = 4;
            }
            if(DbColumnTypeConstant.INT_8 == columnType) {
                columnLength = 8;
            }
        }
        return new Column(columnName, columnType, columnIndex, columnLength);
    }






    private String getNextKeyWord() {
        int i = currStartIndex;
        while (i < sqlCharArr.length) {
            if (sqlCharArr[i] == ' ') {
                break;
            }
            if (sqlCharArr[i] == ';') {
                break;
            }
            i++;
        }
        String word = new String(sqlCharArr, currStartIndex, i - currStartIndex);
        currStartIndex = i;
        return word;
    }

    private String getNextOriginalWord() {
        int i = currStartIndex;
        while (i < sqlCharArr.length) {
            if (sqlCharArr[i] == ' ') {
                break;
            }
            if (sqlCharArr[i] == ';') {
                break;
            }
            i++;
        }
        String word = originalSql.substring(currStartIndex, i);
        currStartIndex = i;
        return word;
    }


    private void skipSpace() {
        while (currStartIndex < sqlCharArr.length) {
            if (sqlCharArr[currStartIndex] != ' ') {
                break;
            }
            currStartIndex++;
        }
    }


    @Override
    public String toString() {
        return originalSql.substring(0, currStartIndex) + "[*]" +originalSql.substring(currStartIndex, originalSql.length());
    }
}
