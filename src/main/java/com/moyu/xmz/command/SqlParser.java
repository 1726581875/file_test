package com.moyu.xmz.command;

import com.moyu.xmz.command.ddl.*;
import com.moyu.xmz.command.dml.*;
import com.moyu.xmz.command.dml.expression.*;
import com.moyu.xmz.command.dml.function.FuncArg;
import com.moyu.xmz.command.dml.sql.*;
import com.moyu.xmz.common.constant.*;
import com.moyu.xmz.common.exception.DbException;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.common.exception.SqlExecutionException;
import com.moyu.xmz.common.exception.SqlIllegalException;
import com.moyu.xmz.common.util.AssertUtils;
import com.moyu.xmz.common.util.SqlParserUtils;
import com.moyu.xmz.common.util.TypeConvertUtils;
import com.moyu.xmz.session.ConnectSession;
import com.moyu.xmz.session.Table;
import com.moyu.xmz.store.accessor.IndexMetaAccessor;
import com.moyu.xmz.store.accessor.TableMetaAccessor;
import com.moyu.xmz.store.common.block.TableIndexBlock;
import com.moyu.xmz.store.common.dto.Column;
import com.moyu.xmz.store.common.dto.TableInfo;
import com.moyu.xmz.store.common.dto.SelectColumn;
import com.moyu.xmz.store.common.meta.IndexMeta;
import com.moyu.xmz.store.common.meta.TableMeta;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

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

    private List<Parameter> parameterList = new ArrayList<>();

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
    private static final String DISTINCT = "DISTINCT";
    private static final String FROM = "FROM";
    private static final String WHERE = "WHERE";
    private static final String GROUP = "GROUP";
    private static final String ORDER = "ORDER";
    private static final String OFFSET = "OFFSET";
    private static final String LIMIT = "LIMIT";
    private static final String BY = "BY";
    private static final String AS = "AS";
    private static final String ON = "ON";
    private static final String VALUE = "VALUE";
    private static final String VALUES = "VALUES";
    private static final String JOIN = "JOIN";
    private static final String IS = "IS";
    private static final String NOT = "NOT";
    private static final String NULL = "NULL";
    private static final String DEFAULT = "DEFAULT";



    public SqlParser(ConnectSession connectSession) {
        this.connectSession = connectSession;
    }

    @Override
    public Command prepareCommand(String sql) {

        // 预先处理sql，去掉特殊符号
        String preprocessSql = preprocessSql(sql);

        this.originalSql = preprocessSql;
        this.upperCaseSql = preprocessSql.toUpperCase();
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
                        CreateDatabaseCmd command = new CreateDatabaseCmd(databaseName);
                        return command;
                    // create table
                    case TABLE:
                        return getCreateTableCommand();
                    // CREATE INDEX indexName ON tableName (columnName(length));
                    case INDEX:
                        skipSpace();
                        String indexName = getNextOriginalWord();
                        assertNextKeywordIs(ON);
                        skipSpace();
                        String tableName = parseTableNameOrIndexName();
                        StartEndIndex startEnd = getNextBracketStartEnd();
                        String columnName = originalSql.substring(startEnd.getStart() + 1, startEnd.getEnd());
                        return buildCreateIndexCommand(tableName, columnName, indexName, CommonConstant.GENERAL_INDEX);
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
                        DropDatabaseCmd command = new DropDatabaseCmd();
                        command.setDatabaseName(databaseName);
                        return command;
                        // drop table
                    case TABLE:
                        skipSpace();
                        String keyword99 = getNextKeyWordUnMove();
                        DropTableCmd dropTableCmd = null;
                        if("IF".equals(keyword99)) {
                            assertNextKeywordIs("IF");
                            assertNextKeywordIs("EXISTS");
                            String tableName = getNextOriginalWord();
                            dropTableCmd = new DropTableCmd(this.connectSession.getDatabase(), tableName, true);
                        } else {
                            String tableName = getNextOriginalWord();
                            dropTableCmd =  new DropTableCmd(this.connectSession.getDatabase(), tableName, false);
                        }
                        return dropTableCmd;
                    case INDEX:
                        skipSpace();
                        String indexName = getNextOriginalWord();
                        assertNextKeywordIs(ON);
                        skipSpace();
                        String tableName11 = getNextOriginalWord();
                        TableMeta tableMeta = getTableMeta(tableName11);
                        return new DropIndexCmd(this.connectSession.getDatabaseId(), tableMeta.getTableId(), tableMeta.getTableName(), indexName);
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
                    return new ShowDatabasesCmd();
                    // show tables
                } else if ("TABLES".equals(word11)) {
                    return new ShowTablesCmd(this.connectSession.getDatabaseId());
                } else {
                    ExceptionUtil.throwSqlIllegalException("sql语法有误，在{}附近");
                }
            case DESC:
                skipSpace();
                String word12 = getNextOriginalWord();
                return new DescTableCmd(this.connectSession.getDatabase(), word12);
            case TRUNCATE:
                assertNextKeywordIs(TABLE);
                skipSpace();
                // tableName
                String word13 = getNextOriginalWord();
                return new TruncateTableCmd(connectSession.getDatabaseId() ,word13);
            default:
                ExceptionUtil.throwSqlIllegalException("sql语法有误，在{}附近");
        }

        throw new DbException("sql语法有误");
    }

    /**
     * 预先处理sql
     * 换行符替换为空格
     * @return
     */
    private String preprocessSql(String sql) {
        StringBuilder stringBuilder = new StringBuilder();
        char[] charArray = sql.toCharArray();
        for (int i = 0; i < charArray.length; i++) {
            if(charArray[i] == ';') {
                break;
            }
            if (charArray[i] == '\n') {
                stringBuilder.append(" ");
            } else {
                stringBuilder.append(charArray[i]);
            }
        }
        return stringBuilder.toString();
    }



    private String parseTableNameOrIndexName() {
        skipSpace();
        String tableName = null;
        int i = currIndex;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                ExceptionUtil.throwSqlIllegalException("sql语法有误");
            }
            if (sqlCharArr[currIndex] == '(' || sqlCharArr[currIndex] == ' ') {
                tableName = originalSql.substring(i, currIndex);
                break;
            }
            currIndex++;
        }
        return tableName;
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
                break;
            case "DROP":
            default:
                throw new SqlIllegalException("sql语法有误");
        }
        throw new SqlIllegalException("sql语法有误");
    }


    private CreateIndexCmd parseIndexCommand(String tableName, byte indexType){
        skipSpace();
        String indexName = parseTableNameOrIndexName();
        StartEndIndex startEnd = getNextBracketStartEnd();
        String columnName = originalSql.substring(startEnd.getStart() + 1, startEnd.getEnd());
        CreateIndexCmd command = buildCreateIndexCommand(tableName, columnName, indexName, indexType);
        return command;
    }



    private CreateIndexCmd buildCreateIndexCommand(String tableName, String columnName, String indexName, byte indexType){
        Table table = connectSession.getDatabase().getTable(tableName);
        CreateIndexCmd command = new CreateIndexCmd(new TableInfo(connectSession, table, null));
        command.setIndexName(indexName);
        command.setColumnName(columnName);
        command.setIndexType(indexType);
        return command;
    }




    private UpdateCmd getUpdateCommand() {
        skipSpace();
        String tableName = getNextOriginalWord();
        if(tableName == null) {
            ExceptionUtil.throwSqlIllegalException("sql语法有误,tableName为空");
        }

        Column[] columns = getColumns(tableName);
        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : columns) {
            columnMap.put(c.getColumnName(), c);
        }

        skipSpace();
        String keyword = getNextKeyWord();
        if(!"SET".equals(keyword)) {
            ExceptionUtil.throwSqlIllegalException("sql语法有误");
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
            if (WHERE.equals(word)) {
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
                ExceptionUtil.throwSqlIllegalException("sql语法有误,再{}附近", columnStr);
            }
            String columnNameStr = split[0].trim();
            String columnValueStr = split[1].trim();
            Column column = columnMap.get(columnNameStr);
            if(column == null) {
                ExceptionUtil.throwSqlIllegalException("表{}不存在字段{}",tableName, columnNameStr);
            }
            Column updateColumn = column.createNullValueColumn();
            if("?".equals(columnValueStr)) {
                int size = parameterList.size();
                Parameter parameter = new Parameter(size + 1, null);
                parameterList.add(parameter);
                updateColumn.setValue(parameter);
            } else {
                setColumnValue(updateColumn, columnValueStr);
            }
            updateColumnList.add(updateColumn);
        }


        // 解析where条件
        Expression condition = null;
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if (WHERE.equals(nextKeyWord)) {
            condition = parseWhereCondition(columnMap);
        }
        TableInfo tableInfo = getOperateTableInfo(tableName, columns, condition);
        UpdateCmd updateCmd = new UpdateCmd(tableInfo,updateColumnList.toArray(new Column[0]));
        updateCmd.addParameters(parameterList);
        return updateCmd;
    }



    private DeleteCmd getDeleteCommand() {
        skipSpace();
        String tableName = null;
        while (true) {
            skipSpace();
            if(currIndex >= sqlCharArr.length) {
                ExceptionUtil.throwSqlIllegalException("sql语法有误");
            }
            String word = getNextKeyWord();
            if(FROM.equals(word)) {
                skipSpace();
                tableName = getNextOriginalWord();
                break;
            }
        }

        if(tableName == null) {
            ExceptionUtil.throwSqlIllegalException("sql语法有误,tableName为空");
        }

        Column[] columns = getColumns(tableName);
        Map<String, Column> columnMap = getColumnMap(columns);

        Expression condition = null;
        String nextKeyWord = getNextKeyWord();
        if (WHERE.equals(nextKeyWord)) {
            condition = parseWhereCondition(columnMap);
        }

        TableInfo tableInfo = getOperateTableInfo(tableName, columns, condition);
        DeleteCmd deleteCmd = new DeleteCmd(tableInfo);
        return deleteCmd;
    }


    private TableInfo getOperateTableInfo(String tableName, Column[] columns, Expression condition) {
        List<IndexMeta> indexMetaList = getIndexList(tableName);
        TableMeta tableMeta = getTableMeta(tableName);
        TableInfo tableInfo = new TableInfo(this.connectSession, tableName, columns, condition);
        tableInfo.setAllIndexList(indexMetaList);
        tableInfo.setEngineType(tableMeta.getEngineType());
        return tableInfo;
    }



    private SelectCmd getSelectCommand() {
        // 解析查询sql
        Query query = parseQuery(null);

        query.setSession(connectSession);
        // 对查询进行优化
        SelectCmd selectCmd = new SelectCmd(query);
        if(query.getCondition() != null) {
            // 选择可用索引
            query.getCondition().setSelectIndexes(query);
            // 打印优化前的条件（方便观察）
            System.out.println("原始的条件:" + query.getCondition().getConditionSQL());

            // 优化查询条件
            Expression optimizeCondition = query.getCondition().optimize();
            query.setCondition(optimizeCondition);

            // 打印优化后的条件（方便观察）
            System.out.println("优化后的条件:" + optimizeCondition.getConditionSQL());
        }

        // 下推条件，把where后面条件下推到连接条件(即在进行连接时候尽可能筛选掉不符合条件的行，而不是等到连接完再筛选)
        QueryTable mainTable = query.getMainTable();
        if(mainTable == null) {
            return selectCmd;
        }
        Expression condition = query.getCondition();
        if (mainTable.getJoinTables() != null && mainTable.getJoinTables().size() > 0
                && condition != null) {
            for (QueryTable joinTable : mainTable.getJoinTables()) {
                Expression joinCondition = condition.getJoinCondition(mainTable, joinTable);
                if (joinTable.getJoinCondition() != null) {
                    joinCondition = ConditionAndOr.buildAnd(joinTable.getJoinCondition(), joinCondition);

                }

                // TODO 打印条件，方便观察条件是否正确
                String joinConditionSql = joinCondition != null ? joinCondition.getConditionSQL() : "null";
                System.out.println("表["+ mainTable.getTableName() +"] 与表["+ joinTable.getTableName() +"] " +
                        "进行"+ joinTable.getJoinInType()+"连接. 连接条件为:" + joinConditionSql);

                joinTable.setJoinCondition(joinCondition);
            }
        }

        selectCmd.addParameters(parameterList);

        return selectCmd;
    }



    private Query parseQuery(StartEndIndex subQueryStartEnd) {

        Query query = new Query();
        query.setSession(connectSession);

        skipSpace();
        String tableName = null;
        int columnStartIdx = currIndex;
        int columnEndIdx = currIndex;
        QueryTable mainTable = null;
        Query subQuery = null;
        while (true) {
            if(currIndex >= sqlCharArr.length || sqlCharArr[currIndex] == ';') {
                // 没有FROM关键字，例如：select now();
                columnEndIdx = currIndex;
                break;
            }
            if(subQueryStartEnd != null && currIndex == subQueryStartEnd.getEnd()) {
                columnEndIdx = currIndex;
                break;
            }

            // 遇到FROM关键字
            if(nextKeywordIs(FROM)) {
                columnEndIdx = currIndex;
                nextNextKeywordIs(FROM, true);
                // 存在子查询,类似这种情况select * from (select * from table_1) as tmp
                skipSpace();
                if(sqlCharArr[currIndex] == '(') {
                    StartEndIndex subStartEnd = getNextBracketStartEnd();
                    currIndex++;
                    assertNextKeywordIs(SELECT);
                    // 解析子查询
                    subQuery = parseQuery(subStartEnd);
                    if(sqlCharArr[currIndex] == ')') {
                        currIndex++;
                    }
                    mainTable = getSubQueryAliasMainTable(subQuery);
                    mainTable.setSubQuery(subQuery);
                    mainTable.setJoinTables(new ArrayList<>());
                } else {
                    // 解析表信息
                    mainTable = parseFormTableOperation();
                }
                break;
            }

            currIndex++;
        }

        // 没有查询表的情况，像select now();这种不需要解析表信息可以提前返回
        if(mainTable == null) {
            String selectColumnsStr = originalSql.substring(columnStartIdx, columnEndIdx).trim();
            parseSelectColumnNonTable(query, selectColumnsStr);
            return query;
        }


        tableName = mainTable.getTableName();

        // 合并所有表的所有字段
        Column[] allColumns = mainTable.getTableColumns();
        Column.setColumnTableAlias(allColumns, mainTable.getAlias());
        List<QueryTable> joinTables = mainTable.getJoinTables();
        if(joinTables != null) {
            for (QueryTable joinTable : joinTables) {
                Column[] joinColumns = getColumns(joinTable.getTableName());
                Column.setColumnTableAlias(joinColumns, joinTable.getAlias());
                allColumns = Column.mergeColumns(allColumns, joinColumns);
            }
        }

        // 解析select字段
        String selectColumnsStr = originalSql.substring(columnStartIdx, columnEndIdx).trim();
        if(selectColumnsStr.toUpperCase().startsWith(DISTINCT)) {
            query.setDistinct(true);
            selectColumnsStr = selectColumnsStr.substring(DISTINCT.length()).trim();
        }
        SelectColumn[] selectColumns = getSelectColumns(selectColumnsStr, allColumns, subQuery, mainTable.getAlias());



        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : allColumns) {
            String tableAlias = c.getTableAlias() == null ? "" :  c.getTableAlias() + ".";
            columnMap.put(tableAlias + c.getColumnName(), c);
        }

        for (Column c : allColumns) {
            columnMap.put(c.getColumnName(), c);
        }


        // 解析条件
        Expression condition = null;
        skipSpace();
        String nextKeyWord = getNextKeyWordUnMove();
        if(WHERE.equals(nextKeyWord)) {
            getNextKeyWord();
            // 解析where条件
            condition = parseWhereCondition(columnMap);
            // 条件后面再接limit,如select * from table where column1=0 limit 10
            String nextKeyWord2 = getNextKeyWordUnMove();
            if(LIMIT.equals(nextKeyWord2)) {
                parseOffsetLimit(query);
            } else if (ORDER.equals(nextKeyWord2)) {
                // 解析order by语句
                parseOrderBy(columnMap, query);
            } else if(GROUP.equals(nextKeyWord)) {
                parseGroupBy(columnMap, query);
            }
            // table后面直接接limit,如:select * from table limit 10
        } else if(LIMIT.equals(nextKeyWord)) {
            parseOffsetLimit(query);
        } else if (ORDER.equals(nextKeyWord)) {
            parseOrderBy(columnMap, query);
        } else if(GROUP.equals(nextKeyWord)) {
            parseGroupBy(columnMap, query);
        }

        mainTable.setAllColumns(allColumns);

        query.setMainTable(mainTable);
        query.setSelectColumns(selectColumns);
        query.setCondition(condition);

        // 当前索引列表
        if(mainTable.getSubQuery() == null) {
            List<IndexMeta> indexMetaList = getIndexList(tableName);
            Map<String, IndexMeta> indexMap = new HashMap<>();
            if(indexMetaList != null) {
                for (IndexMeta idx : indexMetaList) {
                    indexMap.put(idx.getColumnName(), idx);
                }
            }
            mainTable.setIndexMap(indexMap);
        }

        return query;
    }

    private void parseSelectColumnNonTable(Query query, String selectStr) {
        String[] selectColumnStrArr = selectStr.split(",");
        List<SelectColumn> selectColumnList = new ArrayList<>();
        for (int i = 0; i < selectColumnStrArr.length; i++) {
            String columnStr = selectColumnStrArr[i].trim();


            // 解析字段名
            String columnNameStr = null;
            if(isFunctionColumn(columnStr)) {
                int columnEnd = columnStr.indexOf(")");
                if (columnEnd == -1) {
                    ExceptionUtil.throwSqlIllegalException("sql语法有误，在{}附近", columnStr);
                }
                columnNameStr = columnStr.substring(0, columnEnd + 1);
            } else {
                String[] split = columnStr.split("\\s+");
                columnNameStr = split[0];
            }

            String alias = null;
            if(columnStr.length() > columnNameStr.length()) {
                String aliasStr = columnStr.substring(columnNameStr.length()).trim();
                String[] split = aliasStr.split("\\s+");
                if (split.length == 1) {
                    alias = split[0];
                } else if (split.length == 2) {
                    if (!AS.equals(split[0].toUpperCase())) {
                        ExceptionUtil.throwSqlIllegalException("sql语法有误，在{}附近", columnStr);
                    }
                    alias = split[1];
                }
            }

            //函数
            if (isFunctionColumn(columnNameStr)) {
                SelectColumn functionExpression = parseFunction(null, columnNameStr);
                functionExpression.setAlias(alias);
                selectColumnList.add(functionExpression);
            }
        }
        query.setSelectColumns(selectColumnList.toArray(new SelectColumn[0]));
    }


    private void parseGroupBy(Map<String, Column> columnMap, Query query) {
        List<GroupField> groupFields = new ArrayList<>();
        assertNextKeywordIs(GROUP);
        assertNextKeywordIs(BY);

        skipSpace();
        int charStart = currIndex;
        int charEnd= currIndex;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                charEnd = sqlCharArr.length;
                break;
            }
            // 遇到limit结束
            if (sqlCharArr[currIndex] == ')'
                    || sqlCharArr[currIndex] == ';'
                    || nextKeywordIs(ORDER)
                    || nextKeywordIs(LIMIT)) {
                charEnd = currIndex;
                break;
            }
            currIndex++;
        }

        if(charEnd > charStart) {
            String groupByStr = originalSql.substring(charStart, charEnd);
            String[] split = groupByStr.split(",");
            for (String columnName : split) {
                Column column = columnMap.get(columnName.trim());
                if(column == null) {
                    ExceptionUtil.throwSqlIllegalException("SQL语法有误，在GROUP BY附近, 字段{}不存在", columnName.trim());
                }
                groupFields.add(new GroupField(column));
            }
        }

        if(groupFields.size() == 0) {
            ExceptionUtil.throwSqlIllegalException("SQL语法有误，在GROUP BY附近：{}", originalSql);
        }

        query.setGroupFields(groupFields);

        if (nextKeywordIs(LIMIT)) {
            parseOffsetLimit(query);
        } else if (nextKeywordIs(ORDER)) {
            parseOrderBy(columnMap, query);
        }
    }

    private void parseOrderBy(Map<String, Column> columnMap, Query query) {
        List<SortField> sortFieldList = new ArrayList<>();
        assertNextKeywordIs(ORDER);
        assertNextKeywordIs(BY);


        skipSpace();
        int charStart = currIndex;
        while (true) {
            if (currIndex > sqlCharArr.length) {
                if (sortFieldList.size() == 0) {
                    ExceptionUtil.throwSqlIllegalException("SQL语法有误，在ORDER BY附近：{}", originalSql);
                }
                break;
            }
            // 排序字段
            if(currIndex == sqlCharArr.length || sqlCharArr[currIndex] == ' ' || sqlCharArr[currIndex] == ',') {
                SortField currSortField = new SortField();
                String columnName = originalSql.substring(charStart, currIndex);
                Column column = columnMap.get(columnName);
                if (column == null) {
                    ExceptionUtil.throwSqlIllegalException("SQL语法有误，在ORDER BY附近, 字段{}不存在", columnName);
                }
                currSortField.setColumn(column);
                sortFieldList.add(currSortField);

                skipSpace();
                charStart = currIndex;
                // 排序规则
                while (true) {
                    if (currIndex > sqlCharArr.length) {
                        // 如果没有指定排序规则，使用默认排序方式
                        if (currSortField != null && currSortField.getType() == null) {
                            currSortField.setType(SortField.DEFAULT_RULE);
                        }
                        break;
                    }

                    // 遇到limit结束
                    if(nextKeywordIs(LIMIT)) {
                        String nextKeyWork = getNextKeyWordUnMove();
                        if(LIMIT.equals(nextKeyWork)) {
                            setSortType(charStart, currSortField);
                            break;
                        }
                    }
                    // 拿到定义的排序规则
                    if (currIndex == sqlCharArr.length || sqlCharArr[currIndex] == ' ' || sqlCharArr[currIndex] == ',') {
                        setSortType(charStart, currSortField);
                        skipSpace();
                        if(currIndex < sqlCharArr.length && sqlCharArr[currIndex] == ',') {
                            currIndex++;
                        }
                        skipSpace();
                        charStart = currIndex;
                        break;
                    }
                    currIndex++;
                }
            }
            // 遇到limit结束
            if (nextKeywordIs(LIMIT)) {
                break;
            }


            currIndex++;
        }

        if(sortFieldList.size() == 0) {
            ExceptionUtil.throwSqlIllegalException("SQL语法有误，在ORDER BY附近：{}", originalSql);
        }

        query.setSortFields(sortFieldList);

        // 如果下一个关键字为limit, 解析limit
        String nextKeyWord3 = getNextKeyWordUnMove();
        if(LIMIT.equals(nextKeyWord3)) {
            parseOffsetLimit(query);
        }
    }

    private void setSortType(int charStart, SortField currSortField) {
        if (currIndex > charStart) {
            String sortType = originalSql.substring(charStart, currIndex).toUpperCase().trim();
            if (!sortType.equals(SortField.RULE_ASC) && !sortType.equals(SortField.RULE_DESC)) {
                ExceptionUtil.throwSqlIllegalException("SQL语法有误，在ORDER BY附近, 排序规则不合法。{}", sortType);
            } else {
                currSortField.setType(sortType);
            }
        } else {
            // 给默认排序方式
            currSortField.setType(SortField.DEFAULT_RULE);
        }
    }


    private Expression parseWhereCondition(Map<String, Column> columnMap) {
        Expression condition = null;
        Expression l = readCondition(columnMap);
        String nextKeyWordUnMove = getNextKeyWordUnMove();
        if(ConditionAndOr.AND.equals(nextKeyWordUnMove)) {
            condition = readRightAndCondition(columnMap, l);
        } else {
            condition = readRightOrCondition(columnMap, l);
        }
        return condition;
    }

    /**
     * 当前sql解析到了[*]位置，拿子查询别名
     * select * from (select * from table_1)[*] as tmp
     * @param subQuery
     * @return
     */
    private QueryTable getSubQueryAliasMainTable(Query subQuery) {
        QueryTable mainTable = null;
        String as = getNextKeyWordUnMove();
        if ("AS".equals(as)) {
            // skip as
            getNextKeyWord();
        }
        String tableName = getNextOriginalWord();
        if ("".equals(tableName)) {
            ExceptionUtil.throwSqlIllegalException("sql语法有误，子查询缺少alias, {}", originalSql.substring(0, currIndex));
        }

        SelectColumn[] selectColumns = subQuery.getSelectColumns();
        Column[] newColumns = new Column[selectColumns.length];
        for (int i = 0; i < selectColumns.length; i++) {
            Byte columnType = selectColumns[i].getColumnType();
            if(columnType == null && selectColumns[i].getColumn() != null) {
                columnType = selectColumns[i].getColumn().getColumnType();
            }
            // todo 查询字段类型如果不明确暂时给-1
            if(columnType == null) {
                columnType = -1;
            }
            Column column = new Column(selectColumns[i].getSelectColumnName(), columnType, i, -1);
            newColumns[i] = column;
            newColumns[i].setTableAlias(tableName);
            newColumns[i].setAlias(selectColumns[i].getAlias());
        }

        mainTable = new QueryTable(tableName, newColumns);
        mainTable.setAlias(tableName);

        return mainTable;
    }



    private void parseOffsetLimit(Query query) {
        getNextKeyWord();
        String limitNum = getNextKeyWord().trim();
        query.setLimit(Integer.valueOf(limitNum));

        skipSpace();
        String offsetStr = getNextKeyWord();
        Integer offset = null;
        if(OFFSET.equals(offsetStr)) {
            skipSpace();
            String offsetNum = getNextKeyWord().trim();
            offset = Integer.valueOf(offsetNum);
        }
        query.setOffset(offset == null ? 0 : offset);
    }


    private QueryTable parseFormTableOperation() {
        Map<String,Column> columnMap = new HashMap<>();
        // 解析查询的表信息
        QueryTable mainTable = parseTableInfo();

        mainTable.setJoinTables(new ArrayList<>());

        Column[] columns = getColumns(mainTable.getTableName());
        Column.setColumnTableAlias(columns, mainTable.getAlias());
        for (Column c : columns) {
            columnMap.put(c.getTableAliasColumnName(), c);
        }

        while (true) {
            skipSpace();
            if (currIndex >= sqlCharArr.length) {
                break;
            }

            String word11 = getNextKeyWordUnMove();
            if (WHERE.equals(word11) || LIMIT.equals(word11) || GROUP.equals(word11)
                    || currIndex >= sqlCharArr.length) {
                break;
            }

            if (sqlCharArr[currIndex] == ')') {
                currIndex++;
                break;
            }
            // 判断连接类型
            if (CommonConstant.JOIN_TYPE_INNER.equals(word11)
                    || CommonConstant.JOIN_TYPE_LEFT.equals(word11)
                    || CommonConstant.JOIN_TYPE_RIGHT.equals(word11)) {
                // skip keyword INNER/LEFT/RIGHT
                getNextKeyWord();
                // skip keyword JOIN
                assertNextKeywordIs(JOIN);

                // 解析连接表信息
                QueryTable joinTable = parseTableInfo();
                Column[] columns2 = getColumns(joinTable.getTableName());
                Column.setColumnTableAlias(columns2, joinTable.getAlias());
                for (Column c : columns2) {
                    columnMap.put(c.getTableAliasColumnName(), c);
                }
                // skip keyword ON
                assertNextKeywordIs(ON);
                // 解析连接条件
                Expression condition = parseWhereCondition(columnMap);
                joinTable.setJoinCondition(condition);
                joinTable.setJoinInType(word11);
                mainTable.getJoinTables().add(joinTable);
            } else if(",".equals(word11)){
                assertNextKeywordIs(",");
                QueryTable joinTable = parseTableInfo();
                Column[] columns2 = getColumns(joinTable.getTableName());
                Column.setColumnTableAlias(columns2, joinTable.getAlias());
                for (Column c : columns2) {
                    columnMap.put(c.getTableAliasColumnName(), c);
                }
                joinTable.setJoinInType(CommonConstant.JOIN_TYPE_INNER);
                mainTable.getJoinTables().add(joinTable);
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
    private QueryTable parseTableInfo() {

        String tableName = getNextOriginalWord();

        if(tableName.endsWith(")")) {
            tableName = tableName.substring(0, tableName.length() - 1);
            currIndex--;
        }

        Column[] columns = getColumns(tableName);
        TableMeta tableMeta = getTableMeta(tableName);
        QueryTable table = new QueryTable(tableName, columns);
        table.setEngineType(tableMeta.getEngineType());

        String next = getNextKeyWordUnMove();
        if(AS.equals(next)) {
            getNextKeyWord();
            String alias = getNextOriginalWord();
            table.setAlias(alias);
            Column.setColumnTableAlias(table.getTableColumns(), table.getAlias());
        } else if(!GROUP.equals(next)
                && !LIMIT.equals(next)
                && !WHERE.equals(next)
                && !ORDER.equals(next)) {
            String alias;
            if(next == null) {
                alias = tableName;
            } else {
                alias = getNextOriginalWord();
                if(alias.length() > 1 && alias.endsWith(")")) {
                    alias = alias.substring(0, alias.length() - 1);
                    currIndex--;
                } else if(")".equals(alias)) {
                    alias = tableName;
                    //currIndex--;
                }
            }
            table.setAlias(alias);
            Column.setColumnTableAlias(table.getTableColumns(), table.getAlias());
        }

        return table;
    }





    private void assertNextKeywordIs(String keyword) {
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        AssertUtils.assertTrue(keyword.equals(nextKeyWord), "Sql语法有误,在附近" + nextKeyWord);
    }


    private List<IndexMeta> getIndexList(String tableName) {
        IndexMetaAccessor metadataStore = null;
        try {
            TableMeta tableMeta = getTableMeta(tableName);
            metadataStore = new IndexMetaAccessor(connectSession.getDatabaseId());
            Map<Integer, TableIndexBlock> columnMap = metadataStore.getIndexMap();
            TableIndexBlock tableIndexBlock = columnMap.get(tableMeta.getTableId());
            if (tableIndexBlock != null) {
                return tableIndexBlock.getIndexMetaList();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(metadataStore != null) {
                metadataStore.close();
            }
        }
        return null;
    }





    private SelectColumn[] getSelectColumns(String selectColumnsStr, Column[] allColumns, Query subQuery, String tbAlias) {

        if(subQuery != null) {
            allColumns = SelectColumn.getColumnBySelectColumn(subQuery);
            Column.setColumnTableAlias(allColumns, tbAlias);
        }

        TableColumnInfo columnInfo = new TableColumnInfo();
        for (Column c : allColumns) {
            columnInfo.setColumn(new Column[]{c}, c.getTableAlias());
        }

        // 解析select关键字后面字段信息
        String[] selectColumnStrArr = selectColumnsStr.split(",");
        List<SelectColumn> selectColumnList = new ArrayList<>();
        for (int i = 0; i < selectColumnStrArr.length; i++) {
            String str = selectColumnStrArr[i].trim();
            // SELECT * 或者 SELECT a.*
            if("*".equals(str) || str.endsWith(".*")) {
                String[] split = str.split("\\.");
                String tableAlias = null;
                if(split.length == 2) {
                    tableAlias = split[0];
                }
                for (int j = 0; j < allColumns.length; j++) {
                    Column column = allColumns[j].copy();
                    SelectColumn selectColumn = new SelectColumn(column, column.getColumnName());
                    selectColumn.setTableAlias(column.getTableAlias());
                    selectColumn.setAlias(column.getAlias());
                    if(tableAlias == null) {
                        selectColumnList.add(selectColumn);
                    } else {
                        // 适配这种情况 select t.* from (select * from test_table) t
                        if(subQuery != null) {
                            selectColumn.setAlias(tableAlias);
                            selectColumnList.add(selectColumn);
                        } else if (tableAlias.equals(column.getTableAlias())) {
                            selectColumnList.add(selectColumn);
                        }
                    }
                }
            } else {

                String alias = null;

                // 解析字段名
                String columnStr = null;
                if(isFunctionColumn(str)) {
                    int columnEnd = str.indexOf(")");
                    if (columnEnd == -1) {
                        ExceptionUtil.throwSqlIllegalException("sql语法有误，在{}附近", str);
                    }
                    columnStr = str.substring(0, columnEnd + 1);
                } else {
                    String[] split = str.split("\\s+");
                    columnStr = split[0];
                }
                /*
                  处理字段alias
                  可能情况 columnName AS aliasName 、columnName aliasName
                          COUNT(name) AS aliasName、COUNT(DISTINCT name) AS aliasName
                 */
                if(str.length() > columnStr.length()) {
                    String aliasStr = str.substring(columnStr.length()).trim();
                    String[] split = aliasStr.split("\\s+");
                    if (split.length == 1) {
                        alias = split[0];
                    } else if (split.length == 2) {
                        if (!AS.equals(split[0].toUpperCase())) {
                            ExceptionUtil.throwSqlIllegalException("sql语法有误，在{}附近", str);
                        }
                        alias = split[1];
                    }
                }


                SelectColumn selectColumn = null;
                //函数
                if (isFunctionColumn(columnStr)) {
                    selectColumn = parseFunction(columnInfo, columnStr);
                } else {
                    // 普通字段
                    Column column = columnInfo.getColumn(columnStr);
                    if (column == null) {
                        ExceptionUtil.throwSqlIllegalException("字段{}不存在", columnStr);
                    }

                    column = column.copy();

                    String cTableAlias = null;
                    String cName = null;
                    String[] columnSplit = columnStr.split("\\.");
                    if(columnSplit.length == 2){
                        cTableAlias = columnSplit[0];
                        cName = columnSplit[1];
                    } else {
                        cName = columnSplit[0];
                    }

                    selectColumn = new SelectColumn(column, cName);
                    selectColumn.setTableAlias(cTableAlias);
                }
                selectColumn.setAlias(alias);
                selectColumnList.add(selectColumn);
            }
        }
        return selectColumnList.toArray(new SelectColumn[0]);
    }


    private boolean isFunctionColumn(String functionStr) {
        return FuncNameEnum.isFunction(functionStr);
    }


    private SelectColumn parseFunction(TableColumnInfo columnInfo, String functionStr) {
        String selectColumnStr = functionStr.trim();

        Column column = null;
        String selectColumnName = selectColumnStr;
        String[] args = null;

        int i = functionStr.indexOf('(');
        if(i == -1) {
            ExceptionUtil.throwSqlIllegalException("sql不合法，在{}附近", functionStr);
        }
        String functionName = functionStr.substring(0, i).toUpperCase();


        List<FuncArg> funcArgList = new LinkedList<>();
        SelectColumn selectColumn = null;
        Byte resultType = null;
        switch (functionName) {
            case FuncConstant.FUNC_COUNT:
                resultType = DbTypeConstant.INT_8;
                args = new String[1];
                String arg = getFunctionArg(functionStr);
                if ("*".equals(arg)) {
                    args[0] = "*";
                    break;
                }
                args = new String[1];
                String columnName0 = getFunctionArg(functionStr);
                String[] argColumns = columnName0.split("\\s+");
                String cName = argColumns.length > 1 ? argColumns[1] : argColumns[0];
                column = columnInfo.getColumn(cName);
                if (column == null) {
                    ExceptionUtil.throwSqlIllegalException("sql不合法，字段{}不存在", cName);
                }
                args[0] = columnName0;
                break;
            case FuncConstant.FUNC_MAX:
            case FuncConstant.FUNC_MIN:
            case FuncConstant.FUNC_SUM:
            case FuncConstant.FUNC_AVG:
                resultType = FuncConstant.FUNC_AVG.equals(functionName) ? DbTypeConstant.DOUBLE : DbTypeConstant.INT_8;
                args = new String[1];
                String columnName = getFunctionArg(functionStr);
                column = columnInfo.getColumn(columnName);
                if (column == null) {
                    ExceptionUtil.throwSqlIllegalException("sql不合法，字段{}不存在", columnName);
                }
                args[0] = columnName;
                break;
            case FuncConstant.FUNC_UUID:
                SelectColumnExpr uuidFunc = SelectColumnExpr.newFuncColumnExpr(FuncConstant.FUNC_UUID, null);
                selectColumn = new SelectColumn(selectColumnName, functionName, uuidFunc, DbTypeConstant.CHAR);
               return selectColumn;
            case FuncConstant.FUNC_NOW:
                SelectColumnExpr nowFunc = SelectColumnExpr.newFuncColumnExpr(FuncConstant.FUNC_NOW, null);
                selectColumn = new SelectColumn(selectColumnName, functionName, nowFunc, DbTypeConstant.TIMESTAMP);
                return selectColumn;
            case FuncConstant.UNIX_TIMESTAMP:
                String timeStr = getFunctionArg(functionStr);
                if(timeStr.startsWith("'") && timeStr.endsWith("'")) {
                    timeStr = timeStr.substring(1, timeStr.length() - 1);
                } else {
                    ExceptionUtil.throwSqlIllegalException("函数{}参数不合法, 参数必须为字符串,参数{}", functionName, timeStr);
                }
                funcArgList.add(new FuncArg(0, FuncArg.CONSTANT, timeStr));
                SelectColumnExpr unixTimestampFunc = SelectColumnExpr.newFuncColumnExpr(FuncConstant.UNIX_TIMESTAMP, funcArgList);
                selectColumn = new SelectColumn(selectColumnName, functionName, unixTimestampFunc, DbTypeConstant.INT_8);
                return selectColumn;
            case FuncConstant.FROM_UNIXTIME:
                String timestampStr = getFunctionArg(functionStr);
                if(timestampStr.startsWith("'") && timestampStr.endsWith("'")) {
                    timestampStr = timestampStr.substring(1, timestampStr.length() - 1);
                }
                funcArgList.add(new FuncArg(0, FuncArg.CONSTANT, timestampStr));
                SelectColumnExpr formUnixtimeFunc = SelectColumnExpr.newFuncColumnExpr(FuncConstant.FROM_UNIXTIME, funcArgList);
                selectColumn = new SelectColumn(selectColumnName, functionName, formUnixtimeFunc, DbTypeConstant.CHAR);
                return selectColumn;
            default:
                ExceptionUtil.throwSqlIllegalException("sql不合法，不支持函数{}", functionName);
        }

        selectColumn = new SelectColumn(column, selectColumnName, functionName, args);
        selectColumn.setColumnType(resultType);
        return selectColumn;
    }


    private String getFunctionArg(String functionStr) {
        int start = functionStr.indexOf('(');
        int end = functionStr.indexOf(')');
        if (start < 0 || end < 0 || start > end) {
            ExceptionUtil.throwSqlIllegalException("sql不合法，{}", functionStr);
        }
        return functionStr.substring(start + 1, end).trim();
    }



    public Expression readCondition(Map<String, Column> columnMap) {
        boolean isOpen = false;
        Expression left = null;
        while (true) {
            if (sqlCharArr[currIndex] == ' ') {
                currIndex++;
                continue;
            }
            if (sqlCharArr[currIndex] == '(') {
                currIndex++;
                isOpen = true;
            }
            do {
                // 读左边条件表达式
                left = readLeftExpression(columnMap);
                if(left instanceof ConditionColumnExpr || left instanceof ConstantValue) {
                    left = readRightExpression(columnMap, left);
                }

                if(!isOpen) {
                    break;
                }

                String andOrType = getNextKeyWordUnMove();
                if (ConditionAndOr.AND.equals(andOrType) || ConditionAndOr.OR.equals(andOrType)) {
                    getNextKeyWord();
                    Expression right = readCondition(columnMap);
                    left = new ConditionAndOr(andOrType, left, right);
                }
            } while (isOpen && sqlCharArr[currIndex] != ')');

            if (isOpen && sqlCharArr[currIndex] == ')') {
                currIndex++;
            }
            break;
        }

        return left;
    }


    public Expression readLeftExpression(Map<String, Column> columnMap) {
        Expression l = null;
       while (true) {
        if (sqlCharArr[currIndex] == '(') {
            l = readCondition(columnMap);
            String andOrType = getNextKeyWordUnMove();
            if (ConditionAndOr.AND.equals(andOrType) || ConditionAndOr.OR.equals(andOrType)) {
                getNextKeyWord();
                Expression right = readCondition(columnMap);
                l = new ConditionAndOr(andOrType, l, right);
            }
            break;
        } else {
            l = parseLeftColumnExpression(columnMap);
            break;
        }
       }
        return l;
    }


    public Expression readRightAndCondition(Map<String, Column> columnMap, Expression l) {
        String nextKeyWord = getNextKeyWordUnMove();
        if(!ConditionAndOr.AND.equals(nextKeyWord)) {
            return l;
        }

        getNextKeyWord();
        Expression right = readCondition(columnMap);
        l = new ConditionAndOr(nextKeyWord, l, right);

        String next = getNextKeyWordUnMove();
        if (ConditionAndOr.OR.equals(next)) {
            l = readRightOrCondition(columnMap, l);
        } else {
            l = readRightAndCondition(columnMap, l);
        }
        return l;
    }

    public Expression readRightOrCondition(Map<String, Column> columnMap, Expression l) {

        String nextKeyWord = getNextKeyWordUnMove();
        if (!ConditionAndOr.OR.equals(nextKeyWord)) {
            return l;
        }

        getNextKeyWord();
        Expression right = readCondition(columnMap);
        l = new ConditionAndOr(nextKeyWord, l, right);

        String next = getNextKeyWordUnMove();
        if (ConditionAndOr.AND.equals(next)) {
            l = readRightAndCondition(columnMap, l);
        } else {
            l = readRightOrCondition(columnMap, l);
        }
        return l;
    }



    public Expression parseLeftColumnExpression(Map<String, Column> columnMap) {
        skipSpace();
        int start = currIndex;
        Expression columnExpression = null;
        Column column = null;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
            if ((sqlCharArr[currIndex] == ' '
                    || sqlCharArr[currIndex] == '='
                    || sqlCharArr[currIndex] == '!'
                    || sqlCharArr[currIndex] == '<')) {
                String columnName = originalSql.substring(start, currIndex);
                if (columnName.startsWith("'") && columnName.endsWith("'")) {
                    columnExpression = new ConstantValue(columnName);
                } else if (isNumericString(columnName)) {
                    columnExpression = new ConstantValue(columnName);
                } else {
                    column = columnMap.get(columnName);
                    if (column == null) {
                        ExceptionUtil.throwSqlIllegalException("sql语法有误，字段不存在：{}", columnName);
                    }
                    column = column.copy();
                    columnExpression = new ConditionColumnExpr(column);
                }

                skipSpace();
                break;
            }
            currIndex++;
        }

        if(columnExpression == null) {
            ExceptionUtil.throwDbException("解析sql发生异常，sql:{}", originalSql);
        }


        return columnExpression;
    }

    private Expression readRightExpression(Map<String, Column> columnMap, Expression left) {
        Expression condition = null;
        String operator = getNextOperatorKeyWord();
        skipSpace();
        switch (operator) {
            case OperatorConstant.EQUAL:
            case OperatorConstant.NOT_EQUAL_1:
            case OperatorConstant.NOT_EQUAL_2:
            case OperatorConstant.LIKE:
            case OperatorConstant.LESS_THAN:
            case OperatorConstant.LESS_THAN_OR_EQUAL:
            case OperatorConstant.GREATER_THAN:
            case OperatorConstant.GREATER_THAN_OR_EQUAL:
                condition = parseSingleComparison(columnMap, left, operator);
                break;
            case OperatorConstant.IN:
                condition = parseInCondition(left, true);
                break;
            case OperatorConstant.EXISTS:
                break;
            case "NOT":
                skipSpace();
                String word0 = getNextKeyWord();
                if ("IN".equals(word0)) {
                    condition = parseInCondition(left, true);
                } else if ("LIKE".equals(word0)) {
                    condition = parseSingleComparison(columnMap, left, OperatorConstant.NOT_LIKE);
                } else if ("EXISTS".equals(word0)) {
                    throw new DbException("目前不支持EXISTS查询");
                }
                break;
            case IS:
                String word1 = getNextKeyWord();
                if (NULL.equals(word1)) {
                    condition = new SingleComparison(OperatorConstant.IS_NULL, left, new ConstantValue(null));
                } else if ("NOT".equals(word1)) {
                    condition = new SingleComparison(OperatorConstant.IS_NOT_NULL, left, new ConstantValue(null));
                }
                break;
            case OperatorConstant.BETWEEN:
                String lowerLimit = parseSimpleConditionValue(currIndex);
                assertNextKeywordIs("AND");
                skipSpace();
                String upperLimit = parseSimpleConditionValue(currIndex);

                Object lowValue = getTypeValueObj(left, lowerLimit);
                Object upValue = getTypeValueObj(left, upperLimit);
                condition = new ConditionBetween(left, new ConstantValue(lowValue),  new ConstantValue(upValue));
                break;
            default:
                throw new SqlIllegalException("sql语法有误");
        }
        // 校验是否合法
        if (condition == null) {
            throw new SqlIllegalException("sql语法有误");
        }

        return condition;
    }


    private String getNextOperatorKeyWord() {
        StringBuilder operator = new StringBuilder("");
        skipSpace();

        // 不等于 !=
        if (sqlCharArr[currIndex] == '!') {
            operator.append(sqlCharArr[currIndex]);
            operator.append(sqlCharArr[++currIndex]);
            currIndex++;
            return operator.toString();
        }
        // 等于=
        if (sqlCharArr[currIndex] == '=') {
            operator.append(sqlCharArr[currIndex]);
            currIndex++;
            return operator.toString();
        }
        // < 、<=或<>
        if (sqlCharArr[currIndex] == '<') {
            operator.append(sqlCharArr[currIndex]);
            if (sqlCharArr[currIndex + 1] == '>' || sqlCharArr[currIndex + 1] == '=') {
                operator.append(sqlCharArr[++currIndex]);
            }
            currIndex++;
            return operator.toString();
        }
        // > 、>=
        if (sqlCharArr[currIndex] == '>') {
            operator.append(sqlCharArr[currIndex]);
            if (sqlCharArr[currIndex + 1] == '=') {
                operator.append(sqlCharArr[++currIndex]);
            }
            currIndex++;
            return operator.toString();
        }

        // IN
        if (sqlCharArr[currIndex] == 'I' && sqlCharArr[currIndex + 1] == 'N') {
            operator.append(sqlCharArr[currIndex]);
            currIndex++;
            operator.append(sqlCharArr[currIndex]);
            currIndex++;
            return operator.toString();

        }

        // 其他，例如IS 、NOT
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
            if (sqlCharArr[currIndex] == ' ') {
                currIndex++;
                break;
            }
            operator.append(sqlCharArr[currIndex]);
            currIndex++;
        }
        return operator.toString();
    }


    private SingleComparison parseSingleComparison(Map<String,Column> columnMap, Expression left, String operator){
        SingleComparison condition = null;
        skipSpace();
        boolean isSubQuery = sqlCharArr[currIndex] == '(' ? true : false;

        Expression right = null;
        if (isSubQuery) {
            StartEndIndex subStartEnd = getNextBracketStartEnd();
            currIndex++;
            assertNextKeywordIs(SELECT);
            // 解析子查询
            Query subQuery = parseQuery(subStartEnd);
            right = new SubQueryValue((ConditionColumnExpr) left, subQuery);
        } else {
            String nextValue = getNextOriginalWordUnMove();
            Column rightColumn = columnMap.get(nextValue);
            // attribute = value
            if (rightColumn == null) {
                String v = parseSimpleConditionValue(currIndex);
                if("?".equals(v)) {
                    int size = parameterList.size();
                    Parameter parameter = new Parameter(size + 1, null);
                    parameterList.add(parameter);
                    right = new ConstantValue(parameter);
                } else {
                    // 转换值为对应类型对象
                    Object obj = getTypeValueObj(left, v);
                    right = new ConstantValue(obj);
                }

            } else {
                // attribute = attribute
                String columnName = getNextOriginalWord();
                Column column = columnMap.get(columnName);
                if (column == null) {
                    ExceptionUtil.throwSqlIllegalException("sql语法有误，字段不存在:{}", columnName);
                }
                column = column.copy();
                right = new ConditionColumnExpr(column);
            }
        }
        condition = new SingleComparison(operator, left, right);

        return condition;
    }

    private Object getTypeValueObj(Expression left, String value){
        Object obj = null;
        if(left instanceof ConditionColumnExpr) {
            ConditionColumnExpr c = (ConditionColumnExpr) left;
            Column column = c.getColumn();
            obj = TypeConvertUtils.convertValueType(value, column.getColumnType());
        } else {
            obj = value;
        }
        return obj;
    }



    public boolean isNumericString(String input) {
        try {
            Double.parseDouble(input); // 使用Double类的parseDouble方法尝试将字符串转换为数字
            return true; // 转换成功，说明是数字字符串
        } catch (NumberFormatException e) {
            return false; // 转换出错，说明不是数字字符串
        }
    }


    private Expression parseInCondition(Expression left, boolean isIn) {
        Expression condition = null;
        List<Expression> values = new ArrayList<>();
        StartEndIndex startEnd = getNextBracketStartEnd();
        String inValueStr = originalSql.substring(startEnd.getStart() + 1, startEnd.getEnd());
        if (!(inValueStr.toUpperCase().startsWith(SELECT))) {
            String[] split = inValueStr.split(",");
            for (String v : split) {
                String value = v.trim();
                if (v.startsWith("'") && value.endsWith("'") && value.length() > 1) {
                    value = value.substring(1, value.length() - 1);
                }
                // 转换值为对应类型对象
                Object obj = null;
                if(left instanceof ConditionColumnExpr) {
                    ConditionColumnExpr c = (ConditionColumnExpr) left;
                    Column column = c.getColumn();
                    obj = TypeConvertUtils.convertValueType(value, column.getColumnType());
                } else {
                    obj = value;
                }
                values.add(new ConstantValue(obj));
            }
            currIndex = startEnd.getEnd();
            condition = new ConditionIn(isIn, left, values);
        } else {
            currIndex = startEnd.getStart() + 1;
            assertNextKeywordIs(SELECT);
            Query query = parseQuery(startEnd);
            condition = new ConditionInSubQuery(isIn, left, query);
        }
        return condition;
    }

    private String parseSimpleConditionValue(Integer start) {
        String value = null;
        // 标记字符串 "'" 符号是否打开状态
        boolean strOpen = false;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
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
                value = originalSql.substring(start, currIndex);
                currIndex++;
                break;
                // 数值结束
            } else if ((isEndChar(sqlCharArr[currIndex]) || currIndex == sqlCharArr.length - 1) && !strOpen) {
                // 数值就是最后一个
                if ((currIndex == sqlCharArr.length - 1) && !isEndChar(sqlCharArr[currIndex])) {
                    value = originalSql.substring(start, sqlCharArr.length);
                } else {
                    value = originalSql.substring(start, currIndex);
                }
                break;
            }

            currIndex++;
        }

        return value;
    }




    private boolean isEndChar(char c) {
        return (c == ' ' || c == ')' || c == ';');
    }



    /**
     * insert into xmz_table(id, name) value (1, 'xmz');
     * @return
     */
    private InsertCmd getInsertCommand() {

        skipSpace();

        String word = getNextKeyWord();
        if (!"INTO".equals(word)) {
            ExceptionUtil.throwSqlIllegalException("sql语法有误,{}", word);
        }

        // 解析tableName
        String tableName = SqlParserUtils.getUnquotedStr(parseTableNameOrIndexName());

        Column[] tableColumns = getColumns(tableName);

        // 解析字段
        String[] columnNames = null;
        if(nextKeywordIs(VALUE) || nextKeywordIs(VALUES)) {
            columnNames = new String[tableColumns.length];
            for (int i = 0; i < tableColumns.length; i++) {
                columnNames[i] = tableColumns[i].getColumnName();
            }
        } else {
            Set<String> existsColumnNameSet = Arrays.stream(tableColumns).map(Column::getColumnName).collect(Collectors.toSet());
            // 读取字段
            StartEndIndex columnBracket = getNextBracketStartEnd();
            String columnStr = originalSql.substring(columnBracket.getStart() + 1, columnBracket.getEnd());
            columnNames = columnStr.split(",");

            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i].trim();
                if((columnName.startsWith("`") && columnName.endsWith("`"))
                        || (columnName.startsWith("\"") && columnName.endsWith("\""))) {
                    columnName = columnName.substring(1, columnName.length() - 1);
                }
                if(!existsColumnNameSet.contains(columnName)) {
                    ExceptionUtil.throwSqlIllegalException("表{}不存在字段{}", tableName, columnName);
                }
                columnNames[i] = columnName;
            }
            currIndex = columnBracket.getEnd() + 1;
        }

        // 读字段值
        skipSpace();
        String valueKeyWord = getNextKeyWord();
        if (!VALUE.equals(valueKeyWord) && !VALUES.equals(valueKeyWord)) {
            ExceptionUtil.throwSqlIllegalException("sql语法有误,{}", valueKeyWord);
        }
        // value
        StartEndIndex valueBracket = getNextBracketStartEnd();
        String valueStr = originalSql.substring(valueBracket.getStart() + 1, valueBracket.getEnd());
        String[] valueList = valueStr.split(",");


        if (columnNames.length != valueList.length) {
            throw new SqlIllegalException("sql语法有误");
        }

        Map<String, String> columnValueMap = new HashMap<>();
        Map<String, Integer> paramIndexMap = new HashMap<>();
        for (int j = 0; j < columnNames.length; j++) {
            columnValueMap.put(columnNames[j].trim(), valueList[j].trim());
            paramIndexMap.put(columnNames[j].trim(), j + 1);
        }

        // 插入字段赋值
        Column[] columns = tableColumns;
        Column[] dataColumns = new Column[columns.length];

        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i].copy();
            String value = columnValueMap.get(column.getColumnName());
            if("?".equals(value)) {
                // 获取参数位置
                Integer paramIndex = paramIndexMap.get(column.getColumnName());
                if(paramIndex == null) {
                    paramIndex = parameterList.size() + 1;
                }
                Parameter parameter = new Parameter(paramIndex, null);
                parameterList.add(parameter);
                column.setValue(parameter);
            } else {
                setColumnValue(column, value);
            }
            dataColumns[i] = column;
        }

        TableInfo tableInfo = getOperateTableInfo(tableName, columns, null);
        InsertCmd insertCmd = new InsertCmd(tableInfo, dataColumns);
        insertCmd.addParameters(parameterList);
        return insertCmd;
    }


    private void setColumnValue(Column column, String value) {

        if(column.getDefaultVal() != null && isNullValue(value)) {
            value = column.getDefaultVal();
        }

        if(column.getIsNotNull() == 1 && isNullValue(value)) {
            ExceptionUtil.throwSqlIllegalException("字段{}不能为空", column.getColumnName());
        }

        switch (column.getColumnType()) {
            case DbTypeConstant.TINY_INT:
                Byte byteValue = isNullValue(value) ? null : Byte.valueOf(value);
                column.setValue(byteValue);
                break;
            case DbTypeConstant.INT_4:
                Integer intValue =isNullValue(value) ? null : Integer.valueOf(value);
                column.setValue(intValue);
                break;
            case DbTypeConstant.INT_8:
                Long longValue = isNullValue(value) ? null : Long.valueOf(value);
                column.setValue(longValue);
                break;
            case DbTypeConstant.UNSIGNED_INT_4:
            case DbTypeConstant.UNSIGNED_INT_8:
                BigInteger bigIntValue = isNullValue(value) ? null : new BigInteger(value);
                column.setValue(bigIntValue);
                break;
            case DbTypeConstant.VARCHAR:
            case DbTypeConstant.CHAR:
                if(isNullValue(value)) {
                    column.setValue(null);
                } else if (value.startsWith("'") && value.endsWith("'")) {
                    column.setValue(value.substring(1, value.length() - 1));
                } else {
                    throw new SqlIllegalException("sql不合法，" + value);
                }
                break;
            case DbTypeConstant.TIMESTAMP:
                if (isNullValue(value)) {
                    column.setValue(null);
                } else if (value.startsWith("'") && value.endsWith("'")) {
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String dateStr = value.substring(1, value.length() - 1);
                    try {
                        Date date = dateFormat.parse(dateStr);
                        column.setValue(date);
                    } catch (ParseException e) {
                        e.printStackTrace();
                        throw new SqlIllegalException("日期格式不正确，格式必须是:yyyy-MM-dd HH:mm:ss。当前值:" + dateStr);
                    }

                } else {
                    throw new SqlIllegalException("sql不合法，" + value);
                }
                break;
            default:
                throw new SqlIllegalException("不支持该类型:" + column.getColumnType());
        }
    }




    private boolean isNullValue(String value) {
        return value == null  || NULL.equals(value.toUpperCase());
    }

    private Map<String, Column> getColumnMap(Column[] columns) {
        Map<String, Column> columnMap = new HashMap<>();
        for (Column c : columns) {
            columnMap.put(c.getTableAliasColumnName(), c);
        }
        return columnMap;
    }



    private Column[] getColumns(String tableName) {
        Table table = new Table(connectSession.getDatabaseId(), tableName);
        return table.getColumns();
    }


    private TableMeta getTableMeta(String tableName){
        TableMetaAccessor tableMetadata = null;
        try {
            tableMetadata = new TableMetaAccessor(connectSession.getDatabaseId());
            TableMeta table = tableMetadata.getTable(tableName);
            if(table == null) {
                ExceptionUtil.throwSqlExecutionException("表{}不存在", tableName);
            }
            return table;
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(tableMetadata != null) {
                tableMetadata.close();
            }
        }
        throw new SqlExecutionException("表" + tableName + "不存在");
    }



    /**
     * create table xmz_table (id int, name varchar(10));
     * @return
     */
    private CreateTableCmd getCreateTableCommand() {
        // 解析tableName
        String tableName = parseTableNameOrIndexName();
        tableName = SqlParserUtils.getUnquotedStr(tableName);

        // 解析建表字段
        skipSpace();
        List<Column> columnList = new ArrayList<>();

        StartEndIndex bracketStartEnd = getNextBracketStartEnd();
        String allColumnStr = originalSql.substring(bracketStartEnd.getStart() + 1, bracketStartEnd.getEnd());
        String[] columnStrArr = allColumnStr.split(",");
        int columnIndex = 0;
        for (String columnStr : columnStrArr) {
            String trimStr = columnStr.trim();
            // TODO 创建表时候指定主键，当前暂时先不处理这种语法
            if(trimStr.startsWith("PRIMARY") || trimStr.startsWith("UNIQUE")) {
                continue;
            } else {
                Column column = parseCreateTableColumn(columnIndex++, trimStr);
                columnList.add(column);
            }
        }

        currIndex = bracketStartEnd.getEnd() + 1;

        String engineType = null;
        String nextOriginalWord = getNextOriginalWord();
        if(nextOriginalWord.startsWith("ENGINE=")) {
            engineType = nextOriginalWord.substring("ENGINE=".length());
        }
        if(engineType == null || !engineType.equals(CommonConstant.ENGINE_TYPE_YU)) {
            engineType = CommonConstant.ENGINE_TYPE_YAN;
        }
        // 构造创建表命令
        CreateTableCmd command = new CreateTableCmd();
        command.setDatabaseId(connectSession.getDatabaseId());
        command.setTableName(tableName);
        command.setColumnList(columnList);
        command.setEngineType(engineType);
        command.setSession(connectSession);
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

        String[] columnKeyWords = columnStr.trim().split("\\s+");
        if(columnKeyWords.length < 2) {
            ExceptionUtil.throwSqlIllegalException("sql语法异常,{}", columnStr);
        }

        // 解析字段名
        String columnName = SqlParserUtils.getUnquotedStr(columnKeyWords[0]);
        if((columnName.startsWith("`") && columnName.endsWith("`"))
                || (columnName.startsWith("\"") && columnName.endsWith("\""))) {
            columnName = columnStr.substring(1, columnName.length() - 1);
        }

        // 解析字段类型、字段长度。示例:varchar(64)、int
        String columnTypeStr = columnKeyWords[1];

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
        if (type == null) {
            ExceptionUtil.throwSqlIllegalException("不支持类型:{}", typeName);
        }

        int nextWordIdx = 2;
        // 判断是否有unsigned关键字修饰
        if (columnKeyWords.length > nextWordIdx && "UNSIGNED".equals(columnKeyWords[nextWordIdx].toUpperCase())) {
            if (type == DbTypeConstant.INT_4) {
                type = DbTypeConstant.UNSIGNED_INT_4;
            } else if (type == DbTypeConstant.INT_8) {
                type = DbTypeConstant.UNSIGNED_INT_8;
            }
            nextWordIdx++;
        }

        // 如果没有指定字段长度像int、bigint等，要自动给长度
        if(columnLength == -1) {
            if(DbTypeConstant.INT_4 == type) {
                columnLength = 4;
            }
            if(DbTypeConstant.INT_8 == type) {
                columnLength = 8;
            }
        }


        Column column = new Column(columnName, type, columnIndex, columnLength);

        // 解析字段类型后面的字段设置，像column varchar(10) DEFAULT/NOT NULL 或者 id varchar(10) PRIMARY KEY等等情况
        if (columnKeyWords.length > nextWordIdx) {
            String str = columnKeyWords[nextWordIdx].trim().toUpperCase();
            if (str.equals("PRIMARY")) {
                nextWordIdx++;
                if (columnKeyWords.length <= nextWordIdx || !(columnKeyWords[nextWordIdx].trim().toUpperCase().equals("KEY"))) {
                    ExceptionUtil.throwSqlIllegalException("sql不合法 PRIMARY语法有误");
                }
                column.setIsPrimaryKey((byte) 1);
                nextWordIdx++;
            }
        }

        if (columnKeyWords.length > nextWordIdx) {
            String str = columnKeyWords[nextWordIdx].trim().toUpperCase();
            if (NOT.equals(str)) {
                nextWordIdx++;
                if (columnKeyWords.length <= nextWordIdx || !NULL.equals(columnKeyWords[nextWordIdx].toUpperCase())) {
                    ExceptionUtil.throwSqlIllegalException("sql不合法 {}附近，是否为NOT NULL", columnStr);
                }
                column.setIsNotNull((byte) 1);
                nextWordIdx++;
            }
        }

        if (columnKeyWords.length > nextWordIdx) {
            String str = columnKeyWords[nextWordIdx].trim().toUpperCase();
            if (DEFAULT.equals(str)) {
                nextWordIdx++;
                if (columnKeyWords.length <= nextWordIdx) {
                    ExceptionUtil.throwSqlIllegalException("sql不合法 {}附近", columnStr);
                }
                String defaultVal = columnKeyWords[nextWordIdx];
                if (NULL.equals(defaultVal.toUpperCase())) {
                    if(column.getIsNotNull() == (byte) 1) {
                        ExceptionUtil.throwSqlIllegalException("字段{}不能允许为空，检查是否使NOT NULL和DEFAULT NULL同时使用", column.getColumnName());
                    }
                    column.setIsNotNull((byte) 0);
                    column.setDefaultVal(NULL);
                } else {
                    column.setDefaultVal(defaultVal);
                }
                nextWordIdx++;
            }
        }

        // 解析comment字段
        if (columnKeyWords.length > nextWordIdx) {
            String str = columnKeyWords[nextWordIdx].trim();
            if (str.toUpperCase().equals("COMMENT")) {
                nextWordIdx++;
                if (columnKeyWords.length <= nextWordIdx) {
                    ExceptionUtil.throwSqlIllegalException("sql不合法 {}附近", columnStr);
                }
                String comment = columnKeyWords[nextWordIdx];
                if ((comment.startsWith("'") && comment.endsWith("'"))
                        || (comment.startsWith("\"") && comment.endsWith("\""))) {
                    comment = comment.substring(1, comment.length() - 1);
                } else {
                    ExceptionUtil.throwSqlIllegalException("sql不合法 {}附近", columnStr);
                }
                column.setComment(comment);
                nextWordIdx++;
            }
        }

        return column;
    }


    /**
     * 获取下一个括号的开始位置和结束位置
     * @return
     */
    private StartEndIndex getNextBracketStartEnd() {
        skipSpace();
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
            ExceptionUtil.throwSqlIllegalException("sql语法有误");
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
            if (sqlCharArr[i] == '(') {
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

        if(currIndex >= sqlCharArr.length) {
            return "";
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

    private boolean nextKeywordIs(String keyword) {
        return nextNextKeywordIs(keyword, false);
    }

    /**
     *
     * @param keyword 关键字的值
     * @param skip 断言正确后是否跳过这个关键字
     * @return
     */
    private boolean nextNextKeywordIs(String keyword, boolean skip) {

        if (currIndex >= sqlCharArr.length) {
            return false;
        }

        if (sqlCharArr[currIndex] == ' ') {
            skipSpace();
        } else {
            // 关键字前一个必须为空格
            if (currIndex > 0 && sqlCharArr[currIndex - 1] != ' ') {
                return false;
            }
        }
        int i = currIndex;
        while (i < sqlCharArr.length) {
            if (isWordEndChar(sqlCharArr[i])) {
                break;
            }
            i++;
        }
        String word = new String(sqlCharArr, currIndex, i - currIndex);
        if (keyword.equals(word)) {
            if(skip) {
                currIndex = i;
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean isWordEndChar(char c) {
        return c == ' ' || c == ';' || c == '(' || c == ')';
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
