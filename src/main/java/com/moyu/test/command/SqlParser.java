package com.moyu.test.command;

import com.moyu.test.command.dml.condition.Condition;
import com.moyu.test.command.dml.condition.ConditionTree;
import com.moyu.test.command.ddl.*;
import com.moyu.test.command.dml.*;
import com.moyu.test.command.dml.plan.SelectPlan;
import com.moyu.test.command.dml.plan.SqlPlan;
import com.moyu.test.constant.*;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.*;

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

    private static final String ALTER = "ALTER";

    private static final String DROP = "DROP";
    private static final String SHOW = "SHOW";
    private static final String DESC = "DESC";

    private static final String DATABASE = "DATABASE";
    private static final String TABLE = "TABLE";
    private static final String INDEX = "INDEX";


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
                        String databaseName = getNextOriginalWord();
                        CreateDatabaseCommand command = new CreateDatabaseCommand();
                        command.setDatabaseName(databaseName);
                        return command;
                    // create table
                    case TABLE:
                        return getCreateTableCommand();
                    // CREATE INDEX indexName ON tableName (columnName(length));
                    case INDEX:
                        skipSpace();
                        String indexName = getNextOriginalWord();
                        skipSpace();
                        String ON = getNextKeyWord();
                        if(!"ON".equals(ON)) {
                            throw new SqlIllegalException("sql语法有误");
                        }

                        skipSpace();
                        String tableName = null;
                        int i = currIndex;
                        while (true) {
                            if (currIndex >= sqlCharArr.length) {
                                throw new SqlIllegalException("sql语法有误");
                            }
                            if (sqlCharArr[currIndex] == '(' || sqlCharArr[currIndex] == ' ') {
                                tableName = originalSql.substring(i, currIndex);
                                break;
                            }
                            currIndex++;
                        }
                        StartEndIndex startEnd = getNextBracketStartEnd();
                        String columnName = originalSql.substring(startEnd.getStart() + 1, startEnd.getEnd());
                        return getCreateIndexCommand(tableName, columnName, indexName, CommonConstant.GENERAL_INDEX);
                    default:
                        throw new SqlIllegalException("sql语法有误");
                }
            case UPDATE:
                return getUpdateCommand();
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
                    case INDEX:
                        skipSpace();
                        String indexName = getNextOriginalWord();
                        skipSpace();
                        String ON = getNextKeyWord();
                        if(!"ON".equals(ON)) {
                            throw new SqlIllegalException("sql语法有误");
                        }
                        skipSpace();
                        String tableName11 = getNextOriginalWord();
                        TableMetadata tableMeta = getTableMeta(tableName11);
                        return new DropIndexCommand(this.connectSession.getDatabaseId(), tableMeta.getTableId(), tableMeta.getTableName(), indexName);
                    default:
                        throw new SqlIllegalException("sql语法有误");
                }
            case ALTER:
                return getAlterTableCommand();
            case SHOW:
                skipSpace();
                String word11 = getNextKeyWord();
                // show databases
                if ("DATABASES".equals(word11)) {
                    return new ShowDatabasesCommand();
                    // show tables
                } else if ("TABLES".equals(word11)) {
                    return new ShowTablesCommand(this.connectSession.getDatabaseId());
                } else {
                    throw new SqlIllegalException("sql语法有误");
                }
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
                return new TruncateTableCommand(connectSession.getDatabaseId() ,word13);
            default:
                throw new SqlIllegalException("sql语法有误" + firstKeyWord);
        }
    }


    private Command getAlterTableCommand() {
        skipSpace();
        String tableKeyWord = getNextKeyWord();
        if(!TABLE.equals(tableKeyWord)) {
            throw new SqlIllegalException("sql语法有误");
        }

        skipSpace();
        String tableName = getNextOriginalWord();

        skipSpace();
        String operate = getNextKeyWord();
        switch (operate) {
            case "ADD":
                skipSpace();
                String word0 = getNextKeyWord();
                if(INDEX.equals(word0)) {
                    // ALTER TABLE tableName ADD INDEX indexName(columnName);
                    return parseIndexCommand(tableName, CommonConstant.GENERAL_INDEX);
                } else if("PRIMARY".equals(word0)) {
                    // ALTER TABLE tableName ADD PRIMARY KEY indexName(columnName);
                    skipSpace();
                    String word00 = getNextKeyWord();
                    if(!"KEY".equals(word00)) {
                        throw new SqlIllegalException("sql语法有误，" + word0 + "附近");
                    }

                    return parseIndexCommand(tableName, CommonConstant.PRIMARY_KEY);
                }
            case "DROP":
            default:
                throw new SqlIllegalException("sql语法有误");
        }
    }


    private CreateIndexCommand parseIndexCommand(String tableName, byte indexType){
        skipSpace();

        String indexName = null;
        int i = currIndex;
        while (true) {
            if(currIndex >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if(sqlCharArr[currIndex] == '(' || sqlCharArr[currIndex] == ' ') {
                indexName = originalSql.substring(i, currIndex);
                break;
            }
            currIndex++;
        }

        StartEndIndex startEnd = getNextBracketStartEnd();
        String columnName = originalSql.substring(startEnd.getStart() + 1, startEnd.getEnd());

        CreateIndexCommand command = getCreateIndexCommand(tableName, columnName, indexName, indexType);
        return command;
    }



    private CreateIndexCommand getCreateIndexCommand(String tableName,
                                                     String columnName,
                                                     String indexName,
                                                     byte indexType){
        TableMetadata tableMeta = getTableMeta(tableName);
        Column[] columns = getColumns(tableName);

        Column indexColumn = null;
        for (Column c : columns) {
            if(columnName.equals(c.getColumnName())) {
                indexColumn = c;
            }
        }
        CreateIndexCommand command = new CreateIndexCommand();
        command.setDatabaseId(this.connectSession.getDatabaseId());
        command.setTableName(tableName);
        command.setTableId(tableMeta.getTableId());
        command.setIndexName(indexName);
        command.setColumnName(columnName);
        command.setColumns(columns);
        command.setIndexType(indexType);
        command.setIndexColumn(indexColumn);
        return command;
    }




    private UpdateCommand getUpdateCommand() {
        skipSpace();
        String tableName = getNextOriginalWord();
        if(tableName == null) {
            throw new SqlIllegalException("sql语法有误,tableName为空");
        }

        Column[] columns = getColumns(tableName);
        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : columns) {
            columnMap.put(c.getColumnName(), c);
        }

        skipSpace();
        String keyword = getNextKeyWord();
        if(!"SET".equals(keyword)) {
            throw new SqlIllegalException("sql语法有误");
        }

        // 解析更新字段
        int start = currIndex;
        int end = currIndex;
        while (true) {
            skipSpace();
            if (currIndex >= sqlCharArr.length) {
                end = sqlCharArr.length;
                break;
            }
            end = currIndex;
            String word = getNextKeyWord();
            if ("WHERE".equals(word)) {
                break;
            }
        }

        currIndex = end;

        List<Column> updateColumnList = new ArrayList<>();
        String updateColumnStr = originalSql.substring(start, end).trim();
        String[] updateColumnStrArr = updateColumnStr.split(",");
        for (int i = 0; i < updateColumnStrArr.length; i++) {
            String columnStr = updateColumnStrArr[i];
            String[] split = columnStr.split("=");
            if(split.length != 2) {
                throw new SqlIllegalException("sql语法有误");
            }
            String columnNameStr = split[0].trim();
            String columnValueStr = split[1].trim();
            Column column = columnMap.get(columnNameStr);
            if(column == null) {
                throw new SqlIllegalException("表"+ tableName + "不存在字段" + columnNameStr);
            }

            Column updateColumn = column.createNullValueColumn();
            setColumnValue(updateColumn, columnValueStr);
            updateColumnList.add(updateColumn);
        }


        // 解析where条件
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

        return new UpdateCommand(this.connectSession.getDatabaseId(),
                tableName,
                columns,
                updateColumnList.toArray(new Column[0]),
                root);
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
        return new DeleteCommand(connectSession.getDatabaseId(), tableName, columns, root);
    }

    /**
     * TODO 当前直接查询全部，待优化
     * @return
     */
    private SelectCommand getSelectCommand() {

        skipSpace();
        String tableName = null;
        int startIndex = currIndex;
        int endIndex = currIndex;
        while (true) {
            skipSpace();
            if(currIndex >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            endIndex = currIndex;
            String word = getNextKeyWord();
            if("FROM".equals(word)
                    && sqlCharArr[endIndex - 1] == ' '
                    && sqlCharArr[currIndex] == ' ') {
                skipSpace();
                tableName = getNextOriginalWord();
                break;
            }
        }

        if(tableName == null) {
            throw new SqlIllegalException("sql语法有误,tableName为空");
        }
        Column[] allColumns = getColumns(tableName);
        // 解析select字段
        String selectColumnsStr = originalSql.substring(startIndex, endIndex).trim();
        SelectColumn[] selectColumns = getSelectColumns(tableName, selectColumnsStr, allColumns);


        List<ConditionTree> conditionTreeList = new ArrayList<>();
        // 解析条件
        ConditionTree root = new ConditionTree();
        root.setLeaf(false);
        root.setJoinType(ConditionConstant.AND);
        root.setChildNodes(conditionTreeList);

        // limit 和 offset
        Integer limit = null;
        Integer offset = null;

        // GROUP BY
        String groupByColumnName = null;

        skipSpace();
        String nextKeyWord = getNextKeyWord();
        /**
         * 表名后支持以下三种基本情况
         * 1、select * from where c = 1;
         * 2、select * from limit 10 offset 0;
         *
         * 关于group by、order by还不支持
         *
         */
        if("WHERE".equals(nextKeyWord)) {
            skipSpace();
            parseWhereCondition(root);
            // 条件后面再接limit,如select * from table where column1=0 limit 10
            skipSpace();
            String nextKeyWord2 = getNextKeyWord();
            if("LIMIT".equals(nextKeyWord2)) {
                skipSpace();
                String limitNum = getNextKeyWord().trim();
                limit = Integer.valueOf(limitNum);

                skipSpace();
                String offsetStr = getNextKeyWord();
                if("OFFSET".equals(offsetStr)) {
                    skipSpace();
                    String offsetNum = getNextKeyWord().trim();
                    offset = Integer.valueOf(offsetNum);
                }

            } else if ("ORDER".equals(nextKeyWord2)) {

            }
         // table后面直接接limit,如:select * from table limit 10
        } else if("LIMIT".equals(nextKeyWord)) {
            skipSpace();
            String limitNum = getNextKeyWord().trim();
            limit = Integer.valueOf(limitNum);

            skipSpace();
            String offsetStr = getNextKeyWord();
            if("OFFSET".equals(offsetStr)) {
                skipSpace();
                String offsetNum = getNextKeyWord().trim();
                offset = Integer.valueOf(offsetNum);
            }
        } else if ("ORDER".equals(nextKeyWord)) {

        } else if("GROUP".equals(nextKeyWord)) {
            skipSpace();
            String byKeyword = getNextKeyWord();
            if(!"BY".equals(byKeyword)) {
                throw new SqlIllegalException("SQL语法有误");
            }
            skipSpace();
            groupByColumnName = getNextOriginalWord();

        }

        SelectCommand selectCommand = new SelectCommand(connectSession.getDatabaseId(), tableName, allColumns, selectColumns);
        selectCommand.setConditionTree(root);
        selectCommand.setLimit(limit);
        selectCommand.setOffset(offset == null ? 0 : offset);
        selectCommand.setGroupByColumnName(groupByColumnName);


        // 当前索引列表
        List<IndexMetadata> indexMetadataList = getIndexList(tableName);
        // 设置查询计划（是否使用索引）
        SelectPlan selectPlan = SqlPlan.getSelectPlan(root, allColumns, indexMetadataList);
        selectCommand.setSelectPlan(selectPlan);

        return selectCommand;
    }


    private List<IndexMetadata> getIndexList(String tableName) {
        IndexMetadataStore metadataStore = null;
        try {
            TableMetadata tableMeta = getTableMeta(tableName);
            metadataStore = new IndexMetadataStore();
            Map<Integer, TableIndexBlock> columnMap = metadataStore.getIndexMap();
            TableIndexBlock tableIndexBlock = columnMap.get(tableMeta.getTableId());
            if (tableIndexBlock != null) {
                return tableIndexBlock.getIndexMetadataList();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            metadataStore.close();
        }
        return null;
    }





    private SelectColumn[] getSelectColumns(String tableName, String selectColumnsStr, Column[] allColumns) {


        // SELECT *
        if("*".equals(selectColumnsStr)) {
            SelectColumn[] selectColumns = new SelectColumn[allColumns.length];
            for (int i = 0; i < allColumns.length; i++) {
                selectColumns[i] = new SelectColumn(allColumns[i], allColumns[i].getColumnName(), null, null);
            }
            return selectColumns;
        }

        // SELECT column1,column2...
        // SELECT count(*)...
        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : allColumns) {
            columnMap.put(c.getColumnName(), c);
        }
        String[] selectColumnStrArr = selectColumnsStr.split(",");
        SelectColumn[] selectColumns = new SelectColumn[selectColumnStrArr.length];
        for (int i = 0; i < selectColumnStrArr.length; i++) {
            String selectColumnStr = selectColumnStrArr[i].trim();
            Column column = null;
            String selectColumnName = selectColumnStr;
            String functionName = null;
            String[] args = null;

            // count函数
            if (selectColumnStr.startsWith("count(") && selectColumnStr.endsWith(")")) {
                args = new String[1];
                String functionArg = selectColumnStr.substring(6, selectColumnStr.length() - 1).trim();
                if ("*".equals(functionArg)) {
                    args[0] = "*";
                } else {
                    column = columnMap.get(functionArg);
                    if (column == null) {
                        throw new SqlIllegalException("表" + tableName + "不存在字段" + selectColumnStr);
                    }
                    args[0] = functionArg;
                }
                functionName = "count";
                // sum函数
            } else if (selectColumnStr.startsWith("sum(") && selectColumnStr.endsWith(")")) {
                args = new String[1];
                String functionArg = selectColumnStr.substring(4, selectColumnStr.length() - 1).trim();
                column = columnMap.get(functionArg);
                if (column == null) {
                    throw new SqlIllegalException("表" + tableName + "不存在字段" + selectColumnStr);
                }
                args[0] = functionArg;
                functionName = "sum";
                // max函数
            } else if (selectColumnStr.startsWith("max(") && selectColumnStr.endsWith(")")) {
                args = new String[1];
                String functionArg = selectColumnStr.substring(4, selectColumnStr.length() - 1).trim();
                column = columnMap.get(functionArg);
                if (column == null) {
                    throw new SqlIllegalException("表" + tableName + "不存在字段" + selectColumnStr);
                }
                args[0] = functionArg;
                functionName = "max";
                // min函数
            } else if (selectColumnStr.startsWith("min(") && selectColumnStr.endsWith(")")) {
                args = new String[1];
                String functionArg = selectColumnStr.substring(4, selectColumnStr.length() - 1).trim();
                column = columnMap.get(functionArg);
                if (column == null) {
                    throw new SqlIllegalException("表" + tableName + "不存在字段" + selectColumnStr);
                }
                args[0] = functionArg;
                functionName = "min";
                // 字段
            } else {
                column = columnMap.get(selectColumnStr);
                if (column == null) {
                    throw new SqlIllegalException("表" + tableName + "不存在字段" + selectColumnStr);
                }
            }
            selectColumns[i] = new SelectColumn(column, selectColumnName, functionName, args);
        }
        return selectColumns;
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


            // 遇到limit直接结束
            if ("LIMIT".equals(getNextKeyWordUnMove()) && sqlCharArr[currIndex - 1] == ' ') {
                break;
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
            if (currIndex >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[currIndex] == ' ' || sqlCharArr[currIndex] == '(') {
                tableName = originalSql.substring(i, currIndex);
                break;
            }
            currIndex++;
        }

        // ==== 读取字段 ====
        StartEndIndex columnBracket = getNextBracketStartEnd();
        String columnStr = originalSql.substring(columnBracket.getStart() + 1, columnBracket.getEnd());
        String[] columnNameList = columnStr.split(",");
        currIndex =  columnBracket.getEnd() + 1;

        // ==== 读字段值 ===
        skipSpace();
        String valueKeyWord = null;
        i = currIndex;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[currIndex] == ' ' || sqlCharArr[currIndex] == '(') {
                valueKeyWord = upperCaseSql.substring(i, currIndex);
                break;
            }
            currIndex++;
        }
        if (!"VALUE".equals(valueKeyWord)) {
            throw new SqlIllegalException("sql语法有误," + valueKeyWord);
        }
        // value
        StartEndIndex valueBracket = getNextBracketStartEnd();
        String valueStr = originalSql.substring(valueBracket.getStart() + 1, valueBracket.getEnd());
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
            setColumnValue(column, value);
        }


        // 当前索引列表
        List<IndexMetadata> indexMetadataList = getIndexList(tableName);


        return new InsertCommand(connectSession.getDatabaseId(), tableName, columns, indexMetadataList);
    }


    private void setColumnValue(Column column, String value) {
        switch (column.getColumnType()) {
            case DbColumnTypeConstant.INT_4:
                Integer intValue = isNullValue(value) ? null : Integer.valueOf(value);
                column.setValue(intValue);
                break;
            case DbColumnTypeConstant.INT_8:
                column.setValue(Long.valueOf(value));
                break;
            case DbColumnTypeConstant.VARCHAR:
                if (value.startsWith("'") && value.endsWith("'")) {
                    column.setValue(value.substring(1, value.length() - 1));
                } else if(isNullValue(value)) {
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

                } else if(isNullValue(value)) {
                    column.setValue(null);
                }else {
                    throw new SqlIllegalException("sql不合法，" + value);
                }

                break;
            default:
                throw new SqlIllegalException("不支持该类型");
        }
    }




    private boolean isNullValue(String value) {
        return "null".equals(value) || "NULL".equals(value);
    }


    private Column[] getColumns(String tableName) {
        List<ColumnMetadata> columnMetadataList = null;
        TableMetadataStore tableMetadata = null;
        ColumnMetadataStore columnStore = null;
        try {
            tableMetadata = new TableMetadataStore(connectSession.getDatabaseId());
            columnStore = new ColumnMetadataStore();
            TableMetadata table = tableMetadata.getTable(tableName);
            if(table == null) {
                throw new SqlExecutionException("表" + tableName + "不存在");
            }
            TableColumnBlock columnBlock = columnStore.getColumnBlock(table.getTableId());
            columnMetadataList = columnBlock.getColumnMetadataList();
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            columnStore.close();
            tableMetadata.close();
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
            column.setIsPrimaryKey(metadata.getIsPrimaryKey());
            columns[i] = column;
        }

        return columns;
    }


    private TableMetadata getTableMeta(String tableName){
        TableMetadataStore tableMetadata = null;
        try {
            tableMetadata = new TableMetadataStore(connectSession.getDatabaseId());
            TableMetadata table = tableMetadata.getTable(tableName);
            if(table == null) {
                throw new SqlExecutionException("表" + tableName + "不存在");
            }
            return table;
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            tableMetadata.close();
        }
        throw new SqlExecutionException("表" + tableName + "不存在");
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

        // 解析建表字段
        skipSpace();
        List<Column> columnList = new ArrayList<>();

        StartEndIndex bracketStartEnd = getNextBracketStartEnd();
        String allColumnStr = originalSql.substring(bracketStartEnd.getStart() + 1, bracketStartEnd.getEnd());
        String[] columnStrArr = allColumnStr.split(",");
        int columnIndex = 0;
        for (String columnStr : columnStrArr) {
            String trimColumn = columnStr.trim();
            Column column = parseColumn(columnIndex++, trimColumn);
            columnList.add(column);
        }

        // 构造创建表命令
        CreateTableCommand command = new CreateTableCommand();
        command.setDatabaseId(connectSession.getDatabaseId());
        command.setTableName(tableName);
        command.setColumnList(columnList);
        return command;
    }



    static class StartEndIndex{

        private int start;

        private int end;

        public StartEndIndex(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int getStart() {
            return start;
        }

        public void setStart(int start) {
            this.start = start;
        }

        public int getEnd() {
            return end;
        }

        public void setEnd(int end) {
            this.end = end;
        }
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
        String columnTypeStr = columnKeyWord[1];

        int columnLength = -1;
        // 括号开始
        int start = -1;
        // 括号结束
        int end = -1;
        for (int i = 0; i < columnTypeStr.length(); i++) {
            if(columnTypeStr.charAt(i) == '('){
                start = i;
            }
            if(columnTypeStr.charAt(i) == ')'){
                end = i;
            }
        }

        String typeName = null;
        if(end > start && start > 0) {
            columnLength = Integer.valueOf(columnTypeStr.substring(start + 1, end));
            typeName = columnTypeStr.substring(0, start);
        } else {
            typeName = columnTypeStr;
        }
        Byte type = ColumnTypeEnum.getColumnTypeByName(typeName);
        if(type == null) {
            throw new SqlIllegalException("不支持类型:" + typeName);
        }

        // 如果没有指定字段长度像int、bigint等，要自动给长度
        if(columnLength == -1) {
            if(DbColumnTypeConstant.INT_4 == type) {
                columnLength = 4;
            }
            if(DbColumnTypeConstant.INT_8 == type) {
                columnLength = 8;
            }
        }


        Column column = new Column(columnName, type, columnIndex, columnLength);

        // 解析字段类型后面的字段设置，像column varchar(10) DEFAULT/NOT NULL 或者 id varchar(10) PRIMARY KEY等等情况
        if (columnKeyWord.length >= 3) {
            String str = columnKeyWord[2].trim();
            if (str.equals("PRIMARY") || str.equals("primary")) {
                if (columnKeyWord.length < 4 || !(columnKeyWord[3].trim().equals("KEY")|| columnKeyWord[3].trim().equals("key"))) {
                    throw new SqlIllegalException("sql不合法 PRIMARY语法有误");
                }
                column.setIsPrimaryKey((byte) 1);
            } else if (str.equals("NOT")) {

            } else if (str.equals("DEFAULT")) {

            }
        }

        return column;
    }


    private StartEndIndex getNextBracketStartEnd() {
        boolean firstIsOpen = false;
        boolean afterIsOpen = false;
        int startPos = -1;
        int endPos = -1;
        int i = currIndex;
        while (true) {
            if (i >= sqlCharArr.length) {
                break;
            }
            // 遇到第一个括号，标记第一个括号打开状态
            if (sqlCharArr[i] == '(' && !firstIsOpen) {
                firstIsOpen = true;
                startPos = i;
            }
            // 之后再有括号标记之后的括号为打开状态
            else if(sqlCharArr[i] == '(' && firstIsOpen) {
                afterIsOpen = true;
            } else if(sqlCharArr[i] == ')' && afterIsOpen) {
                afterIsOpen = false;
            } else if (sqlCharArr[i] == ')' && !afterIsOpen && firstIsOpen) {
                endPos = i;
                break;
            }
            i++;
        }

        if(startPos == -1 || endPos == -1) {
            throw new SqlIllegalException("sql语法有误");
        }
        return new StartEndIndex(startPos, endPos);
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
