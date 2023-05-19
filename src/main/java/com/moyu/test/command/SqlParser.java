package com.moyu.test.command;

import com.moyu.test.command.condition.Condition;
import com.moyu.test.command.condition.ConditionTree;
import com.moyu.test.command.ddl.*;
import com.moyu.test.command.dml.DeleteCommand;
import com.moyu.test.command.dml.InsertCommand;
import com.moyu.test.command.dml.SelectCommand;
import com.moyu.test.command.dml.TruncateTableCommand;
import com.moyu.test.constant.ColumnTypeEnum;
import com.moyu.test.constant.ConditionConstant;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.constant.OperatorConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.ColumnMetadata;
import com.moyu.test.store.metadata.obj.TableColumnBlock;
import com.moyu.test.store.metadata.obj.TableMetadata;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    private int currIndex;

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
        this.currIndex = 0;
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
                return getDeleteCommand();
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


    private DeleteCommand getDeleteCommand() {
        skipSpace();
        String tableName = null;
        while (true) {
            skipSpace();
            if(currIndex >= sqlCharArr.length) {
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

        ConditionTree root = null;
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if("WHERE".equals(nextKeyWord)) {
            skipSpace();
            root = new ConditionTree();
            root.setLeaf(false);
            root.setJoinType(ConditionConstant.AND);
            root.setChildNodes(new ArrayList<>());
            parseWhereCondition(root);
        }
        Column[] columns = getColumns(tableName);
        return new DeleteCommand(tableName, columns, root);
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
            if(currIndex >= sqlCharArr.length) {
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


        List<ConditionTree> conditionTreeList = new ArrayList<>();
        // 解析条件
        ConditionTree root = new ConditionTree();
        root.setLeaf(false);
        root.setJoinType(ConditionConstant.AND);
        root.setChildNodes(conditionTreeList);
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if("WHERE".equals(nextKeyWord)) {
            skipSpace();
            parseWhereCondition(root);
        } else if("LIMIT".equals(nextKeyWord)) {

        } else if ("ORDER".equals(nextKeyWord)) {

        } else {
            // TODO
        }


        Column[] columns = getColumns(tableName);
        return new SelectCommand(tableName, columns, root);
    }


    /**
     * a = '1' and b=1
     * @param conditionTree
     * @return
     */
    private ConditionTree parseWhereCondition(ConditionTree conditionTree) {

        List<ConditionTree> childNodes = conditionTree.getChildNodes();
        String nextJoinType = ConditionConstant.AND;
        boolean currConditionOpen = false;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
            // 读到开始括号，创建一个条件树节点
            if (sqlCharArr[currIndex] == '(' && !currConditionOpen) {
                ConditionTree newNode = new ConditionTree();
                newNode.setJoinType(nextJoinType);
                newNode.setChildNodes(new ArrayList<>());
                childNodes.add(newNode);
                currIndex++;
                parseWhereCondition(newNode);
                currConditionOpen = true;
                if (currIndex >= sqlCharArr.length) {
                    break;
                }
            }


            // 预先读关键字，判断是AND还是OR,
            skipSpace();
            String nextKeyWord = getNextKeyWordUnMove();
            if (ConditionConstant.AND.equals(nextKeyWord)) {
                nextJoinType = ConditionConstant.AND;
                // skip keyword
                getNextKeyWord();
                skipSpace();
            } else if (ConditionConstant.OR.equals(nextKeyWord)) {
                nextJoinType = ConditionConstant.OR;
                // skip keyword
                getNextKeyWord();
                skipSpace();
            }

            // 读到开始括号，创建一个条件树节点
            if (sqlCharArr[currIndex] == '(' && !currConditionOpen) {
                ConditionTree newNode = new ConditionTree();
                newNode.setJoinType(nextJoinType);
                newNode.setChildNodes(new ArrayList<>());
                childNodes.add(newNode);
                currIndex++;
                parseWhereCondition(newNode);
                currConditionOpen = true;
                if (currIndex >= sqlCharArr.length) {
                    break;
                }
            }



            if (sqlCharArr[currIndex] == ')') {
                break;
            }
            // 条件开始
            if (sqlCharArr[currIndex] != ' ' && sqlCharArr[currIndex] != '(') {
                // 解析条件
                Condition condition = parseCondition();
                // 构造条件树的叶子节点
                ConditionTree node = new ConditionTree();
                node.setLeaf(true);
                node.setJoinType(nextJoinType);
                node.setCondition(condition);
                childNodes.add(node);

                // 遇到结束括号，直接结束当前循环
                skipSpace();
                if (currIndex >= sqlCharArr.length) {
                    break;
                }
                if (sqlCharArr[currIndex] == ')') {
                    currIndex++;
                    break;
                    // 同一括号内还有其他条件，继续解析
                } else if(ConditionConstant.AND.equals(getNextKeyWordUnMove())
                        || ConditionConstant.OR.equals(getNextKeyWordUnMove())) {
                    // 下一个关键字是AND或者OR，currIndex不进行移动。否则下次循环读下一个关键字(AND/OR)变成了"ND"和"R"。
                    continue;
                }
            }
            currIndex++;
        }
        return conditionTree;
    }


    private Condition parseCondition() {
        skipSpace();
        int start = currIndex;
        Condition condition = new Condition();
        String columnName = null;
        String operator = null;
        List<String> values = new ArrayList<>();
        // 1表示下一个是字段、2表示下一个为算子、3表示下一个为值
        int flag = 1;
        // 标记字符串 "'" 符号是否打开状态
        boolean strOpen = false;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
            // flag=1，当前为字段字段名
            if (flag == 1 &&
                    (sqlCharArr[currIndex] == ' '
                            || sqlCharArr[currIndex] == '='
                            || sqlCharArr[currIndex] == '!'
                            || sqlCharArr[currIndex] == '<')) {
                columnName = originalSql.substring(start, currIndex);
                flag = 2;
                skipSpace();
                start = currIndex;
                continue;
            }

            // flag=1，当前为算子
            if (flag == 2) {
                String nextKeyWord = getNextKeyWord();
                switch (nextKeyWord) {
                    case OperatorConstant.EQUAL:
                    case OperatorConstant.NOT_EQUAL_1:
                    case OperatorConstant.NOT_EQUAL_2:
                    case OperatorConstant.IN:
                    case OperatorConstant.EXISTS:
                    case OperatorConstant.LIKE:
                        operator = nextKeyWord;
                        break;
                    case "NOT":
                        skipSpace();
                        String word0 = getNextKeyWord();
                        if ("IN".equals(word0)) {
                            operator = OperatorConstant.NOT_IN;
                        } else if ("LIKE".equals(word0)) {
                            operator = OperatorConstant.NOT_LIKE;
                        } else if ("EXISTS".equals(word0)) {
                            operator = OperatorConstant.NOT_EXISTS;
                        }
                        break;
                    case "IS":
                        skipSpace();
                        String word1 = getNextKeyWord();
                        if ("NULL".equals(word1)) {
                            operator = OperatorConstant.IS_NULL;
                        } else if ("NOT".equals(word1)) {
                            skipSpace();
                            String word2 = getNextKeyWord();
                            if ("NULL".equals(word2)) {
                                operator = OperatorConstant.IS_NOT_NULL;
                            }
                        }
                        break;
                    default:
                        throw new SqlIllegalException("sql语法有误");
                }
                flag = 3;
                skipSpace();
                start = currIndex;
                continue;
            }

            // flag=3，当前为值
            if (flag == 3) {

                if (OperatorConstant.EQUAL.equals(operator)
                        || OperatorConstant.NOT_EQUAL_1.equals(operator)
                        || OperatorConstant.NOT_EQUAL_2.equals(operator)
                        || OperatorConstant.LIKE.equals(operator)) {

                    // 字符串开始
                    if (sqlCharArr[currIndex] == '\'' && !strOpen) {
                        strOpen = true;
                        if (currIndex + 1 < sqlCharArr.length) {
                            start = currIndex + 1;
                        } else {
                            throw new SqlIllegalException("sql语法有误");
                        }
                    } else if (sqlCharArr[currIndex] == '\'' && strOpen) {
                        // 字符串结束
                        values.add(originalSql.substring(start, currIndex));
                        currIndex++;
                        break;
                        // 数值结束
                    } else if ((isEndChar(sqlCharArr[currIndex]) || currIndex == sqlCharArr.length - 1) && !strOpen) {
                        // 数值就是最后一个
                        if((currIndex == sqlCharArr.length - 1) && !isEndChar(sqlCharArr[currIndex])) {
                            values.add(originalSql.substring(start, sqlCharArr.length));
                        } else {
                            values.add(originalSql.substring(start, currIndex));
                        }
                        break;
                    }
                }
            }
            currIndex++;
        }

        // 校验是否合法
        if (columnName == null || operator == null) {
            throw new SqlIllegalException("sql语法有误");
        }
        if(!OperatorConstant.IS_NULL.equals(operator)
                && !OperatorConstant.IS_NOT_NULL.equals(operator)
                && values.size() == 0) {
            throw new SqlIllegalException("sql语法有误");
        }

        condition.setKey(columnName);
        condition.setOperator(operator);
        condition.setValue(values);

        return condition;
    }


    private boolean isEndChar(char c) {
        return (c == ' ' || c == ')' || c == ';');
    }



    private boolean isOperator(String str) {


        return false;
    }


    private int getNextIndex(char c) {
        int i = currIndex;
        while (true) {
            if (i >= sqlCharArr.length) {
                return -1;
            }
            if (c == sqlCharArr[i]) {
                return i;
            }
            i++;
        }
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
        int i = currIndex;
        while (true) {
            if (i >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[i] == ' ' || sqlCharArr[i] == '(') {
                tableName = originalSql.substring(currIndex, i);
                currIndex = i;
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
                currIndex = i + 1;
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

        int i2 = currIndex;
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
                currIndex = i2 + 1;
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
                case DbColumnTypeConstant.INT_8:
                    column.setValue(Long.valueOf(value));
                    break;
                case DbColumnTypeConstant.VARCHAR:
                    if (value.startsWith("'") && value.endsWith("'")) {
                        column.setValue(value.substring(1, value.length() - 1));
                    } else if("null".equals(value) || "NULL".equals(value)) {
                        column.setValue(null);
                    }else {
                        throw new SqlIllegalException("sql不合法，" + value);
                    }
                    break;
                case DbColumnTypeConstant.TIMESTAMP:
                    if (value.startsWith("'") && value.endsWith("'")) {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String dateStr = value.substring(1, value.length() - 1);
                        try {
                            Date date = dateFormat.parse(dateStr);
                            column.setValue(date);
                        } catch (ParseException e) {
                            e.printStackTrace();
                            throw new SqlIllegalException("日期格式不正确，格式必须是:yyyy-MM-dd HH:mm:ss。当前值:" + dateStr);
                        }

                    } else if("null".equals(value) || "NULL".equals(value)) {
                        column.setValue(null);
                    }else {
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
        int i = currIndex;
        while (true) {
            if(i >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[i] == ' ' || sqlCharArr[i] == '(') {
                tableName = originalSql.substring(currIndex, i);
                currIndex = i;
                break;
            }
            i++;
        }

        // 解析columns
        skipSpace();
        List<Column> columnList = new ArrayList<>();
        int idx = currIndex;
        while (true) {
            if(idx >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            // create table (
            // 解析"("里面内容
            if(sqlCharArr[idx] == '(') {
                idx++;
                currIndex++;
                int columnIndex = 0;
                boolean columnNotOpen = true;
                while (true) {
                    skipSpace();
                    if (idx >= sqlCharArr.length) {
                        break;
                    }

                    if ((sqlCharArr[idx] == ')' && columnNotOpen)) {
                        Column column = parseColumn(columnIndex, originalSql.substring(currIndex, idx));
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
                        Column column = parseColumn(columnIndex, originalSql.substring(currIndex, idx));
                        columnList.add(column);
                        columnIndex++;
                        currIndex = idx + 1;
                    }
                    idx++;
                }
            }

            if(sqlCharArr[idx] == ')') {
                break;
            }
            idx++;
            currIndex++;
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
        int i = currIndex;
        while (i < sqlCharArr.length) {
            if (sqlCharArr[i] == ' ') {
                break;
            }
            if (sqlCharArr[i] == ';') {
                break;
            }
            i++;
        }
        String word = new String(sqlCharArr, currIndex, i - currIndex);
        currIndex = i;
        return word;
    }

    private String getNextKeyWordUnMove() {
        int i = currIndex;
        while (i < sqlCharArr.length) {
            if (sqlCharArr[i] == ' ') {
                break;
            }
            if (sqlCharArr[i] == ';') {
                break;
            }
            i++;
        }
        return new String(sqlCharArr, currIndex, i - currIndex);
    }

    private String getNextOriginalWord() {
        int i = currIndex;
        while (i < sqlCharArr.length) {
            if (sqlCharArr[i] == ' ') {
                break;
            }
            if (sqlCharArr[i] == ';') {
                break;
            }
            i++;
        }
        String word = originalSql.substring(currIndex, i);
        currIndex = i;
        return word;
    }


    private void skipSpace() {
        while (currIndex < sqlCharArr.length) {
            if (sqlCharArr[currIndex] != ' ') {
                break;
            }
            currIndex++;
        }
    }


    @Override
    public String toString() {
        return originalSql.substring(0, currIndex) + "[*]" +originalSql.substring(currIndex, originalSql.length());
    }
}
