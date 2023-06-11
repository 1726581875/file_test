package com.moyu.test.command;

import com.moyu.test.command.ddl.*;
import com.moyu.test.command.dml.*;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.command.dml.plan.SelectIndex;
import com.moyu.test.command.dml.plan.SqlPlan;
import com.moyu.test.constant.*;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.*;
import com.moyu.test.util.AssertUtil;

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
                        assertNextKeywordIs("ON");
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
                        assertNextKeywordIs("ON");
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
                assertNextKeywordIs("TABLE");
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
                    assertNextKeywordIs("KEY");
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
        ConditionTree2 root = null;
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if("WHERE".equals(nextKeyWord)) {
            skipSpace();
            root = new ConditionTree2();
            root.setLeaf(false);
            root.setJoinType(ConditionConstant.AND);
            root.setChildNodes(new ArrayList<>());
            parseWhereCondition2(root, getColumnMap(columns));
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

        Column[] columns = getColumns(tableName);

        ConditionTree2 root = null;
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if("WHERE".equals(nextKeyWord)) {
            skipSpace();
            root = new ConditionTree2();
            root.setLeaf(false);
            root.setJoinType(ConditionConstant.AND);
            root.setChildNodes(new ArrayList<>());
            parseWhereCondition2(root, getColumnMap(columns));
        }

        return new DeleteCommand(connectSession.getDatabaseId(), tableName, columns, root);
    }


    /**
     * TODO 待优化
     * @return
     */
    private SelectCommand getSelectCommand() {
        Query query = parseQuery();
        SelectCommand selectCommand = new SelectCommand(connectSession.getDatabaseId(), query);
        return selectCommand;
    }



    private Query parseQuery() {

        Query query = new Query();

        skipSpace();
        String tableName = null;
        int startIndex = currIndex;
        int endIndex = currIndex;
        FromTable mainTable = null;
        Query subQuery = null;
        while (true) {
            skipSpace();
            if(currIndex >= sqlCharArr.length) {
                throw new SqlIllegalException("sql语法有误");
            }
            endIndex = currIndex;
            String word = getNextKeyWord();
            // 解析form后面 至 where前面这段语句。可能会有join操作
            if("FROM".equals(word) && sqlCharArr[endIndex - 1] == ' ' && sqlCharArr[currIndex] == ' ') {
                String nextWord = getNextKeyWordUnMove();
                // 存在子查询
                if ("(".equals(nextWord) || "(SELECT".equals(nextWord)) {
                    currIndex++;
                    String nextKeyWord = getNextKeyWord();
                    if(!SELECT.equals(nextKeyWord)) {
                        throw new SqlIllegalException("sql语法有误");
                    }
                    subQuery = parseQuery();
                    String tbName = getNextOriginalWord();
                    mainTable = new FromTable(tbName, subQuery.getMainTable().getAllColumns(), null);
                    mainTable.setSubQuery(subQuery);
                    mainTable.setJoinTables(new ArrayList<>());
                } else {

                    // 解析表信息
                    mainTable = getFormTableOperation();
                }
                break;
            }
        }


        tableName = mainTable.getTableName();

        // 合并所有表的所有字段
        Column[] allColumns = mainTable.getTableColumns();
        Column.setColumnAlias(allColumns, mainTable.getAlias());
        List<FromTable> joinTables = mainTable.getJoinTables();
        if(joinTables != null) {
            for (FromTable joinTable : joinTables) {
                Column[] joinColumns = getColumns(joinTable.getTableName());
                Column.setColumnAlias(joinColumns, joinTable.getAlias());
                allColumns = Column.mergeColumns(allColumns, joinColumns);
            }
        }

        // 解析select字段
        String selectColumnsStr = originalSql.substring(startIndex, endIndex).trim();
        SelectColumn[] selectColumns = getSelectColumns(selectColumnsStr, allColumns);


        List<ConditionTree2> conditionTreeList = new ArrayList<>();
        // 解析条件
        ConditionTree2 conditionRoot = new ConditionTree2();
        conditionRoot.setLeaf(false);
        conditionRoot.setJoinType(ConditionConstant.AND);
        conditionRoot.setChildNodes(conditionTreeList);

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

            Map<String, Column> columnMap = new HashMap<>();
            for (Column c : allColumns) {
                String tableAlias = c.getTableAlias() == null ? "" :  c.getTableAlias() + ".";
                columnMap.put(tableAlias + c.getColumnName(), c);
            }
            parseWhereCondition2(conditionRoot, columnMap);
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
            assertNextKeywordIs("BY");
            skipSpace();
            groupByColumnName = getNextOriginalWord();

        }

        mainTable.setAllColumns(allColumns);


        query.setMainTable(mainTable);
        query.setSelectColumns(selectColumns);
        query.setConditionTree(conditionRoot);
        query.setLimit(limit);
        query.setOffset(offset == null ? 0 : offset);
        query.setGroupByColumnName(groupByColumnName);


        // 当前索引列表
        if(mainTable.getSubQuery() == null) {
            List<IndexMetadata> indexMetadataList = getIndexList(tableName);
            // 设置查询计划（是否使用索引）
            SelectIndex selectIndex = SqlPlan.getSelectPlan(conditionRoot, allColumns, indexMetadataList);
            query.setSelectIndex(selectIndex);
        }

        return query;
    }


    private FromTable getFormTableOperation() {
        Map<String,Column> columnMap = new HashMap<>();
        FromTable mainTable = getTableInfo();
        mainTable.setJoinTables(new ArrayList<>());


        Column[] columns = getColumns(mainTable.getTableName());
        Column.setColumnAlias(columns, mainTable.getAlias());
        for (Column c : columns) {
            columnMap.put(c.getTableAliasColumnName(), c);
        }

        while (true) {
            skipSpace();
            if (currIndex >= sqlCharArr.length) {
                break;
            }

            String word11 = getNextKeyWordUnMove();
            if ("WHERE".equals(word11) || "LIMIT".equals(word11) || "GROUP".equals(word11)
                    || currIndex >= sqlCharArr.length) {
                break;
            }
            // 判断连接类型
            if (CommonConstant.JOIN_TYPE_INNER.equals(word11)
                    || CommonConstant.JOIN_TYPE_LEFT.equals(word11)
                    || CommonConstant.JOIN_TYPE_RIGHT.equals(word11)) {
                String joinType = getNextKeyWord();
                String join = getNextKeyWord();
                if (!"JOIN".equals(join)) {
                    throw new SqlIllegalException("sql语法有误");
                }

                FromTable joinTable = getTableInfo();
                mainTable.getJoinTables().add(joinTable);


                Column[] columns2 = getColumns(joinTable.getTableName());
                Column.setColumnAlias(columns2, joinTable.getAlias());
                for (Column c : columns2) {
                    columnMap.put(c.getTableAliasColumnName(), c);
                }

                String word12 = getNextKeyWord();
                if (!"ON".equals(word12)) {
                    throw new SqlIllegalException("sql语法有误");
                }
                Condition2 condition = parseJoinCondition(columnMap);
                // todo 目前只支持单条件连接
                ConditionTree2 joinCondiTree = new ConditionTree2();
                joinCondiTree.setLeaf(true);
                joinCondiTree.setJoinType(ConditionConstant.AND);
                joinCondiTree.setCondition(condition);
                joinTable.setJoinCondition(joinCondiTree);
                joinTable.setJoinInType(word11);
            } else {
                break;
            }
        }
        return mainTable;
    }


    /**
     * 解析表信息，表名、表别名
     * @return
     */
    private FromTable getTableInfo() {

        String tableName = getNextOriginalWord();

        Column[] columns = getColumns(tableName);
        FromTable table = new FromTable(tableName, columns, null);

        String next = getNextKeyWordUnMove();
        if("AS".equals(next)) {
            getNextKeyWord();
            String alias = getNextOriginalWord();
            table.setAlias(alias);
            Column.setColumnAlias(table.getTableColumns(), table.getAlias());
        } else if(!"GROUP".equals(next)
                && !"LIMIT".equals(next)
                && !"WHERE".equals(next) && !")".equals(next)) {
            String alias;
            if(next == null) {
                alias = tableName;
            } else {
                alias = getNextOriginalWord();
            }
            table.setAlias(alias);
            Column.setColumnAlias(table.getTableColumns(), table.getAlias());
        }

        return table;
    }





    private void assertNextKeywordIs(String keyword) {
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        AssertUtil.assertTrue(keyword.equals(nextKeyWord), "Sql语法有误,在附近" + nextKeyWord);
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





    private SelectColumn[] getSelectColumns(String selectColumnsStr, Column[] allColumns) {
        // SELECT *
        if("*".equals(selectColumnsStr)) {
            SelectColumn[] selectColumns = new SelectColumn[allColumns.length];
            for (int i = 0; i < allColumns.length; i++) {
                Column column = allColumns[i];
                SelectColumn selectColumn = new SelectColumn(column, column.getColumnName(), null, null);
                selectColumn.setTableAlias(column.getTableAlias());
                selectColumns[i] = selectColumn;
            }
            return selectColumns;
        }

        // SELECT column1,column2...
        // SELECT count(*)...
        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : allColumns) {
            String tableAlias = c.getTableAlias() == null ? "" :  c.getTableAlias() + ".";
            columnMap.put(tableAlias + c.getColumnName(), c);
        }
        String[] selectColumnStrArr = selectColumnsStr.split(",");
        SelectColumn[] selectColumns = new SelectColumn[selectColumnStrArr.length];
        for (int i = 0; i < selectColumnStrArr.length; i++) {
            String str = selectColumnStrArr[i].trim();
            // columnName AS aliasName
            String[] split = str.split("\\s+");
            String columnStr = split[0];
            String alias = null;
            if(split.length == 2) {
                alias = split[1];
            } else if(split.length == 3) {
                if(!"AS".equals(split[1].toUpperCase())) {
                    throw new SqlIllegalException("sql语法有误，在" + str + "附近");
                }
                alias = split[2];
            }

            SelectColumn selectColumn = null;
            //函数
            if (isFunctionColumn(columnStr)){
                selectColumn = parseFunction(columnMap, columnStr);
            } else {
                // 普通字段
                Column column = columnMap.get(columnStr);
                if (column == null) {
                    throw new SqlIllegalException("字段" + columnStr + "不存在");
                }
                selectColumn = new SelectColumn(column, columnStr, null, null);
            }
            selectColumn.setAlias(alias);
            selectColumns[i] = selectColumn;
        }
        return selectColumns;
    }


    private boolean isFunctionColumn(String functionStr) {
        String upperCase = functionStr.toUpperCase();
        if (upperCase.startsWith(FunctionConstant.FUNC_COUNT + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_MAX + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_MIN + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_SUM + "(")) {
            if (upperCase.endsWith(")")) {
                return true;
            }
        }
        return false;
    }


    private SelectColumn parseFunction(Map<String, Column> columnMap, String functionStr) {
        String selectColumnStr = functionStr.trim();

        Column column = null;
        String selectColumnName = selectColumnStr;
        String[] args = null;

        int i = functionStr.indexOf('(');
        if(i == -1) {
            throw new SqlIllegalException("sql不合法，" + functionStr);
        }
        String functionName = functionStr.substring(0, i).toUpperCase();

        switch (functionName) {
            case FunctionConstant.FUNC_COUNT:
                args = new String[1];
                String arg = getFunctionArg(functionStr);
                if ("*".equals(arg)) {
                    args[0] = "*";
                    break;
                }
            case FunctionConstant.FUNC_MAX:
            case FunctionConstant.FUNC_MIN:
            case FunctionConstant.FUNC_SUM:
                args = new String[1];
                String columnName = getFunctionArg(functionStr);;
                column = columnMap.get(columnName);
                if (column == null) {
                    throw new SqlIllegalException("sql不合法，字段" + columnName + "不存在");
                }
                args[0] = columnName;
                break;
            default:
                throw new SqlIllegalException("sql不合法，不支持函数" + functionName);
        }

        SelectColumn selectColumn = new SelectColumn(column, selectColumnName, functionName, args);
        return selectColumn;
    }


    private String getFunctionArg(String functionStr) {
        int start = functionStr.indexOf('(');
        int end = functionStr.indexOf(')');
        if (start < 0 || end < 0 || start > end) {
            throw new SqlIllegalException("sql不合法，" + functionStr);
        }
        return functionStr.substring(start + 1, end).trim();
    }







    /**
     * a = '1' and b=1
     * @param conditionTree
     * @return
     */
    private ConditionTree2 parseWhereCondition2(ConditionTree2 conditionTree, Map<String, Column> columnMap) {

        List<ConditionTree2> childNodes = conditionTree.getChildNodes();
        String nextJoinType = ConditionConstant.AND;
        boolean currConditionOpen = false;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
            // 读到开始括号，创建一个条件树节点
            if (sqlCharArr[currIndex] == '(' && !currConditionOpen) {
                ConditionTree2 childNode = createChildNode2(nextJoinType);
                childNodes.add(childNode);
                currIndex++;
                parseWhereCondition2(childNode, columnMap);
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
                ConditionTree2 newNode = new ConditionTree2();
                newNode.setJoinType(nextJoinType);
                newNode.setChildNodes(new ArrayList<>());
                childNodes.add(newNode);
                currIndex++;
                parseWhereCondition2(newNode, columnMap);
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
                Condition2 condition = parseCondition2(columnMap);
                // 构造条件树的叶子节点
                ConditionTree2 node = new ConditionTree2();
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
            String next = getNextKeyWordUnMove();
            if (("LIMIT".equals(next) || "GROUP".equals(next))
                    && sqlCharArr[currIndex - 1] == ' ') {
                break;
            }

            currIndex++;
        }
        return conditionTree;
    }


    private ConditionTree2 createChildNode2(String joinType) {
        ConditionTree2 newNode = new ConditionTree2();
        newNode.setJoinType(joinType);
        newNode.setChildNodes(new ArrayList<>());
        return newNode;
    }

    private Condition2 parseJoinCondition(Map<String, Column> columnMap) {

        Condition2 condition = parseCondition2(columnMap);


        return condition;
    }


    private Condition2 parseCondition2(Map<String,Column> columnMap) {
        skipSpace();
        int start = currIndex;
        Condition2 condition = null;
        Column column = null;
        String operator = null;
        List<String> values = new ArrayList<>();
        // 1表示下一个是字段、2表示下一个为算子
        int flag = 1;
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
                String columnName = originalSql.substring(start, currIndex);
                column = columnMap.get(columnName);
                if(column == null) {
                    throw new SqlIllegalException("sql语法有误，字段不存在：" + columnName);
                }

                flag = 2;
                skipSpace();
                start = currIndex;
                continue;
            }

            // flag=1，当前为算子
            if (flag == 2) {
                String nextKeyWord = getNextKeyWord();
                skipSpace();
                start = currIndex;
                switch (nextKeyWord) {
                    case OperatorConstant.EQUAL:
                        String nextValue = getNextOriginalWordUnMove();
                        Column rightColumn = columnMap.get(nextValue);
                        if(rightColumn == null) {
                            // attribute = value
                            values = parseConditionValues(start, OperatorConstant.EQUAL);
                            condition = new ConditionEqOrNq(column, values.get(0), true);
                        } else {
                            // attribute = attribute
                            condition = new ConditionLeftRight(column, rightColumn);
                        }
                        break;
                    case OperatorConstant.NOT_EQUAL_1:
                    case OperatorConstant.NOT_EQUAL_2:
                        values = parseConditionValues(start, nextKeyWord);
                        condition = new ConditionEqOrNq(column, values.get(0) , false);
                        break;
                    case OperatorConstant.IN:
                        values = parseConditionValues(start, nextKeyWord);
                        condition = new ConditionInOrNot(column, values , true);
                        break;
                    case OperatorConstant.EXISTS:
                    case OperatorConstant.LIKE:
                        values = parseConditionValues(start, OperatorConstant.LIKE);
                        condition = new ConditionLikeOrNot(column, values.get(0) , true);
                        break;
                    case "NOT":
                        skipSpace();
                        String word0 = getNextKeyWord();
                        if ("IN".equals(word0)) {
                            operator = OperatorConstant.NOT_IN;
                            values = parseConditionValues(start, operator);
                            condition = new ConditionInOrNot(column, values , false);
                        } else if ("LIKE".equals(word0)) {
                            operator = OperatorConstant.NOT_LIKE;
                            values = parseConditionValues(start, operator);
                            condition = new ConditionLikeOrNot(column, values.get(0) , false);
                        } else if ("EXISTS".equals(word0)) {
                            operator = OperatorConstant.NOT_EXISTS;
                        }
                        break;
                    case "IS":
                        skipSpace();
                        String word1 = getNextKeyWord();
                        boolean isNull = true;
                        if ("NULL".equals(word1)) {
                            isNull = true;
                        } else if ("NOT".equals(word1)) {
                            skipSpace();
                            String word2 = getNextKeyWord();
                            if ("NULL".equals(word2)) {
                                isNull = false;
                            }
                        }
                        condition = new ConditionIsNullOrNot(column, isNull);
                        break;
                    default:
                        throw new SqlIllegalException("sql语法有误");
                }

                break;
            }
            currIndex++;
        }

        // 校验是否合法
        if (condition == null) {
            throw new SqlIllegalException("sql语法有误");
        }
/*        if(!OperatorConstant.IS_NULL.equals(operator)
                && !OperatorConstant.IS_NOT_NULL.equals(operator)
                && values.size() == 0) {
            throw new SqlIllegalException("sql语法有误");
        }*/

        return condition;
    }


    private List<String> parseConditionValues(Integer start, String operator) {
        List<String> values = new ArrayList<>();
        // 标记字符串 "'" 符号是否打开状态
        boolean strOpen = false;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
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
                    if ((currIndex == sqlCharArr.length - 1) && !isEndChar(sqlCharArr[currIndex])) {
                        values.add(originalSql.substring(start, sqlCharArr.length));
                    } else {
                        values.add(originalSql.substring(start, currIndex));
                    }
                    break;
                }
            }

            currIndex++;
        }

        return values;
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

    private Map<String, Column> getColumnMap(Column[] columns) {
        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : columns) {
            columnMap.put(c.getTableAliasColumnName(), c);
        }
        return columnMap;
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
            Column column = parseCreateTableColumn(columnIndex++, trimColumn);
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
    private Column parseCreateTableColumn(int columnIndex, String columnStr) {

        String[] columnKeyWord = columnStr.trim().split("\\s+");
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
        skipSpace();
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
        skipSpace();
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
        skipSpace();
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

    private String getNextOriginalWordUnMove() {
        skipSpace();
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
