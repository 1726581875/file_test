package com.moyu.test.command;

import com.moyu.test.command.condition.Condition;
import com.moyu.test.command.ddl.*;
import com.moyu.test.command.dml.InsertCommand;
import com.moyu.test.command.dml.SelectCommand;
import com.moyu.test.command.dml.TruncateTableCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.store.metadata.obj.TableMetadata;

import java.util.*;

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
    private static final String INSERT = "INSERT";
    private static final String TRUNCATE = "TRUNCATE";

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
                return getSelectCommand();
            case INSERT:
                return getInsertCommand();
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
            case TRUNCATE:
                skipSpace();
                String word = getNextKeyWord();
                if(!"TABLE".endsWith(word)) {
                    throw new SqlIllegalException("sql语法有误");
                }
                skipSpace();
                // tableName
                String word13 = getNextOriginalWord();
                return new TruncateTableCommand(word13);
            default:
                throw new SqlIllegalException("sql语法有误");
        }
        return null;
    }


    /**
     * TODO 当前直接查询全部，待优化
     * @return
     */
    private SelectCommand getSelectCommand() {

        skipSpace();
        String tableName = null;
        while (true) {
            skipSpace();
            if(currStartIndex >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            String word = getNextKeyWord();
            if("FROM".equals(word)) {
                skipSpace();
                tableName = getNextOriginalWord();
                break;
            }
        }

        if(tableName == null) {
            throw new SqlIllegalException("sql语法有误,tableName为空");
        }

        // 解析条件
        Condition condition = null;
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if("WHERE".equals(nextKeyWord)) {
            skipSpace();
            String columnName = getNextOriginalWord();
            skipSpace();
            String operator = getNextKeyWord();
            skipSpace();
            String value = getNextOriginalWord();
            if(value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
            condition = new Condition();
            condition.setKey(columnName);
            condition.setOperator(operator);
            condition.setValue(Arrays.asList(value));


        } else if("LIMIT".equals(nextKeyWord)) {

        } else if ("ORDER".equals(nextKeyWord)) {

        } else {
            // TODO
        }


        Column[] columns = getColumns(tableName);
        return new SelectCommand(tableName, columns, condition);
    }


    /**
     * insert into xmz_table(id, name) value (1, 'xmz');
     * @return
     */
    private InsertCommand getInsertCommand() {

        skipSpace();

        String word = getNextKeyWord();
        if (!"INTO".equals(word)) {
            throw new SqlIllegalException("sql语法有误," + word);
        }

        // 解析tableName
        String tableName = null;
        skipSpace();
        int i = currStartIndex;
        while (true) {
            if (i >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[i] == ' ' || sqlCharArr[i] == '(') {
                tableName = originalSql.substring(currStartIndex, i);
                currStartIndex = i;
                break;
            }
            i++;
        }

        // ==== 读取字段 ====
        // 括号开始
        int columnStart = 0;
        // 括号结束
        int columnEnd = 0;
        while (true) {
            if (i >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[i] == '(') {
                columnStart = i;
            }
            if (sqlCharArr[i] == ')') {
                columnEnd = i;
                currStartIndex = i + 1;
                break;
            }
            i++;
        }

        String columnStr = originalSql.substring(columnStart + 1, columnEnd);
        String[] columnNameList = columnStr.split(",");

        // ==== 读字段值 ===
        skipSpace();
        String valueKeyWord = getNextKeyWord();
        if (!"VALUE".equals(valueKeyWord)) {
            throw new SqlIllegalException("sql语法有误," + valueKeyWord);
        }

        int i2 = currStartIndex;
        // 括号开始
        int valueStart = 0;
        // 括号结束
        int valueEnd = 0;
        while (true) {
            if (i2 >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[i2] == '(') {
                valueStart = i2;
            }
            if (sqlCharArr[i2] == ')') {
                valueEnd = i2;
                currStartIndex = i2 + 1;
                break;
            }
            i2++;
        }

        String valueStr = originalSql.substring(valueStart + 1, valueEnd);
        String[] valueList = valueStr.split(",");


        if (columnNameList.length != valueList.length) {
            throw new SqlIllegalException("sql语法有误");
        }
        Map<String, String> columnValueMap = new HashMap<>();
        for (int j = 0; j < columnNameList.length; j++) {
            columnValueMap.put(columnNameList[j].trim(), valueList[j].trim());
        }

        // 插入字段赋值
        Column[] columns = getColumns(tableName);
        for (Column column : columns) {
            String value = columnValueMap.get(column.getColumnName());
            switch (column.getColumnType()) {
                case DbColumnTypeConstant.INT_4:
                    column.setValue(Integer.valueOf(value));
                    break;
                case DbColumnTypeConstant.VARCHAR:
                    if (value.startsWith("'") && value.endsWith("'")) {
                        column.setValue(value.substring(1, value.length() - 1));
                    } else {
                        throw new SqlIllegalException("sql不合法，" + value);
                    }
                    break;
                default:
                    throw new SqlIllegalException("不支持该类型");
            }
        }

        return new InsertCommand(tableName, columns);
    }


    private Column[] getColumns(String tableName) {
        List<ColumnMetadata> columnMetadataList = null;
        TableMetadataStore tableMetadata = null;
        ColumnMetadataStore columnStore = null;
        try {
            tableMetadata = new TableMetadataStore(connectSession.getDatabaseId());
            columnStore = new ColumnMetadataStore();
            TableMetadata table = tableMetadata.getTable(tableName);
            TableColumnBlock columnBlock = columnStore.getColumnBlock(table.getTableId());
            columnMetadataList = columnBlock.getColumnMetadataList();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            columnStore.close();
        }

        if(columnMetadataList == null || columnMetadataList.size() == 0) {
            throw new SqlIllegalException("表缺少字段");
        }

        Column[] columns = new Column[columnMetadataList.size()];

        for (int i = 0; i < columnMetadataList.size(); i++) {
            ColumnMetadata metadata = columnMetadataList.get(i);
            Column column = new Column(metadata.getColumnName(),
                    metadata.getColumnType(),
                    metadata.getColumnIndex(),
                    metadata.getColumnLength());
            columns[i] = column;
        }

        return columns;
    }


    /**
     * create table xmz_table (id int, name varchar(10));
     * @return
     */
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
