package com.moyu.test.command;

import com.moyu.test.command.ddl.*;
import com.moyu.test.command.dml.*;
import com.moyu.test.command.dml.expression.*;
import com.moyu.test.command.dml.plan.Optimizer;
import com.moyu.test.command.dml.sql.*;
import com.moyu.test.command.dml.plan.SelectIndex;
import com.moyu.test.command.dml.plan.SqlPlan;
import com.moyu.test.constant.*;
import com.moyu.test.exception.DbException;
import com.moyu.test.exception.SqlExecutionException;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.session.ConnectSession;
import com.moyu.test.store.metadata.ColumnMetadataStore;
import com.moyu.test.store.metadata.IndexMetadataStore;
import com.moyu.test.store.metadata.TableMetadataStore;
import com.moyu.test.store.metadata.obj.*;
import com.moyu.test.store.operation.OperateTableInfo;
import com.moyu.test.util.AssertUtil;
import com.moyu.test.util.TypeConvertUtil;

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

    private static final String DISTINCT = "DISTINCT";


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
                        CreateDatabaseCommand command = new CreateDatabaseCommand(databaseName);
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
                        String tableName = parseTableNameOrIndexName();
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
                        String keyword99 = getNextKeyWordUnMove();
                        DropTableCommand dropTableCommand = null;
                        if("IF".equals(keyword99)) {
                            assertNextKeywordIs("IF");
                            assertNextKeywordIs("EXISTS");
                            String tableName = getNextOriginalWord();
                            dropTableCommand = new DropTableCommand(this.connectSession.getDatabase(), tableName, true);
                        } else {
                            String tableName = getNextOriginalWord();
                            dropTableCommand =  new DropTableCommand(this.connectSession.getDatabase(), tableName, false);
                        }
                        return dropTableCommand;
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

    private String parseTableNameOrIndexName() {
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
            case "DROP":
            default:
                throw new SqlIllegalException("sql语法有误");
        }
    }


    private CreateIndexCommand parseIndexCommand(String tableName, byte indexType){
        skipSpace();
        String indexName = parseTableNameOrIndexName();
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
        OperateTableInfo tableInfo = new OperateTableInfo(connectSession, tableName, columns, null);
        tableInfo.setEngineType(tableMeta.getEngineType());
        CreateIndexCommand command = new CreateIndexCommand(tableInfo);
        command.setTableId(tableMeta.getTableId());
        command.setIndexName(indexName);
        command.setColumnName(columnName);
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
        Expression condition = null;
        skipSpace();
        String nextKeyWord = getNextKeyWord();
        if ("WHERE".equals(nextKeyWord)) {
            Expression l = readCondition(columnMap);
            condition = readRightCondition(columnMap, l);
        }
        OperateTableInfo operateTableInfo = getOperateTableInfo(tableName, columns, condition);
        UpdateCommand updateCommand = new UpdateCommand(operateTableInfo,updateColumnList.toArray(new Column[0]));
        return updateCommand;
    }

    private ConditionTree parseCondition(Column[] columns) {
        skipSpace();
        ConditionTree conditionTree = new ConditionTree();
        conditionTree.setLeaf(false);
        conditionTree.setJoinType(ConditionConstant.AND);
        conditionTree.setChildNodes(new ArrayList<>());
        parseWhereCondition(conditionTree, getColumnMap(columns), null, null);
        return conditionTree;
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
        Map<String, Column> columnMap = getColumnMap(columns);

        Expression condition = null;
        String nextKeyWord = getNextKeyWord();
        if ("WHERE".equals(nextKeyWord)) {
            Expression l = readCondition(columnMap);
            condition = readRightCondition(columnMap, l);
        }

        OperateTableInfo tableInfo = getOperateTableInfo(tableName, columns, condition);
        DeleteCommand deleteCommand = new DeleteCommand(tableInfo);
        return deleteCommand;
    }


    private OperateTableInfo getOperateTableInfo(String tableName, Column[] columns, Expression condition) {
        List<IndexMetadata> indexMetadataList = getIndexList(tableName);
        TableMetadata tableMeta = getTableMeta(tableName);
        OperateTableInfo tableInfo = new OperateTableInfo(this.connectSession, tableName, columns, condition);
        tableInfo.setIndexList(indexMetadataList);
        tableInfo.setEngineType(tableMeta.getEngineType());
        return tableInfo;
    }



    private SelectCommand getSelectCommand() {
        // 解析查询sql
        Query query = parseQuery(null);
        query.setSession(connectSession);
        // 对查询进行优化
/*        Optimizer optimizer = new Optimizer(query);
        query = optimizer.optimizeQuery();*/
        SelectCommand selectCommand = new SelectCommand(query);
        if(query.getCondition() != null) {
            query.getCondition().setSelectIndexes(query);
        }

        return selectCommand;
    }



    private Query parseQuery(StartEndIndex subQueryStartEnd) {

        Query query = new Query();
        query.setSession(connectSession);

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
            if(subQueryStartEnd != null && currIndex > subQueryStartEnd.getEnd()) {
                throw new SqlIllegalException("sql语法有误");
            }

            endIndex = currIndex;
            String word = getNextKeyWord();
            // 解析form后面 至 where前面这段语句。可能会有join操作
            if("FROM".equals(word) && sqlCharArr[endIndex - 1] == ' ' && sqlCharArr[currIndex] == ' ') {
                String nextWord = getNextKeyWordUnMove();
                // 存在子查询
                // 像select * from [*](select * from table_1) as tmp
                if ("(".equals(nextWord) || "(SELECT".equals(nextWord)) {
                    StartEndIndex subStartEnd = getNextBracketStartEnd();
                    currIndex++;
                    assertNextKeywordIs(SELECT);
                    // 解析子查询
                    subQuery = parseQuery(subStartEnd);
                    mainTable = getSubQueryAliasMainTable(subQuery);
                    mainTable.setSubQuery(subQuery);
                    mainTable.setJoinTables(new ArrayList<>());
                } else {
                    // 解析表信息
                    mainTable = parseFormTableOperation();
                }
                break;
            }
        }


        tableName = mainTable.getTableName();

        // 合并所有表的所有字段
        Column[] allColumns = mainTable.getTableColumns();
        Column.setColumnTableAlias(allColumns, mainTable.getAlias());
        List<FromTable> joinTables = mainTable.getJoinTables();
        if(joinTables != null) {
            for (FromTable joinTable : joinTables) {
                Column[] joinColumns = getColumns(joinTable.getTableName());
                Column.setColumnTableAlias(joinColumns, joinTable.getAlias());
                allColumns = Column.mergeColumns(allColumns, joinColumns);
            }
        }

        // 解析select字段
        String selectColumnsStr = originalSql.substring(startIndex, endIndex).trim();
        if(selectColumnsStr.startsWith("DISTINCT")  || selectColumnsStr.startsWith("distinct")) {
            query.setDistinct(true);
            selectColumnsStr = selectColumnsStr.substring("DISTINCT".length()).trim();
        }
        SelectColumn[] selectColumns = getSelectColumns(selectColumnsStr, allColumns, subQuery, mainTable.getAlias());

        // 解析条件
        //ConditionTree conditionRoot = new ConditionTree(ConditionConstant.AND, new ArrayList<>(), false);
        Expression condition = null;
        // GROUP BY
        String groupByColumnName = null;

        skipSpace();
        String nextKeyWord = getNextKeyWordUnMove();
        /**
         * 表名后支持以下三种基本情况
         * 1、select * from where c = 1;
         * 2、select * from limit 10 offset 0;
         *
         * 关于group by、order by还不支持
         *
         */
        if("WHERE".equals(nextKeyWord)) {
            getNextKeyWord();
            Map<String, Column> columnMap = new HashMap<>();
            for (Column c : allColumns) {
                String tableAlias = c.getTableAlias() == null ? "" :  c.getTableAlias() + ".";
                columnMap.put(tableAlias + c.getColumnName(), c);
            }

            for (Column c : allColumns) {
                columnMap.put(c.getColumnName(), c);
            }

            //parseWhereCondition(conditionRoot, columnMap, null, subQueryStartEnd);
            // 解析where条件
            Expression l = readCondition(columnMap);
            condition = readRightCondition(columnMap, l);

            // 条件后面再接limit,如select * from table where column1=0 limit 10
            skipSpace();
            String nextKeyWord2 = getNextKeyWordUnMove();
            if("LIMIT".equals(nextKeyWord2)) {
                parseOffsetLimit(query);
            } else if ("ORDER".equals(nextKeyWord2)) {

            }
            // table后面直接接limit,如:select * from table limit 10
        } else if("LIMIT".equals(nextKeyWord)) {
            parseOffsetLimit(query);
        } else if ("ORDER".equals(nextKeyWord)) {

        } else if("GROUP".equals(nextKeyWord)) {
            getNextKeyWord();
            assertNextKeywordIs("BY");
            skipSpace();
            groupByColumnName = getNextOriginalWord();
            // select * from (select id,count(*) from xmz_yan group by id) t
            if(groupByColumnName.endsWith(")") && groupByColumnName.length() > 1) {
                groupByColumnName = groupByColumnName.substring(0, groupByColumnName.length() -1);
            } else if(groupByColumnName == "" || groupByColumnName.equals(")")) {
                throw new SqlIllegalException("sql语法有误,在group by附近" + groupByColumnName);
            }

        }

        mainTable.setAllColumns(allColumns);


        query.setMainTable(mainTable);
        query.setSelectColumns(selectColumns);
        query.setCondition(condition);
        query.setGroupByColumnName(groupByColumnName);


        // 当前索引列表
        if(mainTable.getSubQuery() == null) {
            List<IndexMetadata> indexMetadataList = getIndexList(tableName);
            Map<String, Column> columnMap = new HashMap<>();
            for (Column c : mainTable.getTableColumns()) {
                String tableAlias = c.getTableAlias() == null ? "" : c.getTableAlias() + ".";
                columnMap.put(tableAlias + c.getColumnName(), c);
            }

            Map<String, IndexMetadata> indexMap = new HashMap<>();
            if(indexMetadataList != null) {
                for (IndexMetadata idx : indexMetadataList) {
                    indexMap.put(idx.getColumnName(), idx);
                }
            }
            mainTable.setIndexMap(indexMap);
        }

        return query;
    }

    /**
     * 当前sql解析到了[*]位置，拿子查询别名
     * select * from (select * from table_1)[*] as tmp
     * @param subQuery
     * @return
     */
    private FromTable getSubQueryAliasMainTable(Query subQuery) {
        FromTable mainTable = null;
        String as = getNextKeyWordUnMove();
        if ("AS".equals(as)) {
            // skip as
            getNextKeyWord();
        }
        String tableName = getNextOriginalWord();
        if ("".equals(tableName)) {
            throw new SqlIllegalException("sql语法有误，子查询缺少alias, " + originalSql.substring(0, currIndex));
        }
        Column[] subColumns = subQuery.getMainTable().getAllColumns();
        Column[] newColumns = new Column[subColumns.length];
        for (int i = 0; i < subColumns.length; i++) {
            newColumns[i] = subColumns[i].createNullValueColumn();
            newColumns[i].setTableAlias(tableName);
        }

        mainTable = new FromTable(tableName, newColumns, null);
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
        if("OFFSET".equals(offsetStr)) {
            skipSpace();
            String offsetNum = getNextKeyWord().trim();
            offset = Integer.valueOf(offsetNum);
        }
        query.setOffset(offset == null ? 0 : offset);
    }


    private FromTable parseFormTableOperation() {
        Map<String,Column> columnMap = new HashMap<>();
        // 解析查询的表信息
        FromTable mainTable = parseTableInfo();

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
            if ("WHERE".equals(word11) || "LIMIT".equals(word11) || "GROUP".equals(word11)
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
                assertNextKeywordIs("JOIN");

                // 解析连接表信息
                FromTable joinTable = parseTableInfo();
                Column[] columns2 = getColumns(joinTable.getTableName());
                Column.setColumnTableAlias(columns2, joinTable.getAlias());
                for (Column c : columns2) {
                    columnMap.put(c.getTableAliasColumnName(), c);
                }
                // skip keyword ON
                assertNextKeywordIs("ON");

                // todo 目前只支持单条件连接
                // 解析连接条件
                Condition condition = parseJoinCondition(columnMap);
                ConditionTree joinCondiTree = new ConditionTree();
                joinCondiTree.setLeaf(true);
                joinCondiTree.setJoinType(ConditionConstant.AND);
                joinCondiTree.setCondition(condition);
                joinTable.setJoinCondition(joinCondiTree);
                joinTable.setJoinInType(word11);

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
    private FromTable parseTableInfo() {

        String tableName = getNextOriginalWord();

        if(tableName.endsWith(")")) {
            tableName = tableName.substring(0, tableName.length() - 1);
            currIndex--;
        }

        Column[] columns = getColumns(tableName);
        TableMetadata tableMeta = getTableMeta(tableName);
        FromTable table = new FromTable(tableName, columns, null);
        table.setEngineType(tableMeta.getEngineType());

        String next = getNextKeyWordUnMove();
        if("AS".equals(next)) {
            getNextKeyWord();
            String alias = getNextOriginalWord();
            table.setAlias(alias);
            Column.setColumnTableAlias(table.getTableColumns(), table.getAlias());
        } else if(!"GROUP".equals(next)
                && !"LIMIT".equals(next)
                && !"WHERE".equals(next)) {
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





    private SelectColumn[] getSelectColumns(String selectColumnsStr, Column[] allColumns, Query subQuery, String tbAlias) {

        if(subQuery != null) {
            allColumns = SelectColumn.getColumnBySelectColumn(subQuery);
            Column.setColumnTableAlias(allColumns, tbAlias);
        }

        TableColumnInfo columnInfo = new TableColumnInfo();
        for (Column c : allColumns) {
            columnInfo.setColumn(new Column[]{c}, c.getTableAlias());
        }


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
                    SelectColumn selectColumn = new SelectColumn(column, column.getColumnName(), null, null);
                    selectColumn.setTableAlias(column.getTableAlias());
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
                if(isStartWithFunc(str)) {
                    int columnEnd = str.indexOf(")");
                    if (columnEnd == -1) {
                        throw new SqlIllegalException("语法有误，" + str);
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
                        if (!"AS".equals(split[0].toUpperCase())) {
                            throw new SqlIllegalException("sql语法有误，在" + str + "附近");
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
                        throw new SqlIllegalException("字段" + columnStr + "不存在");
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

                    selectColumn = new SelectColumn(column, cName, null, null);
                    selectColumn.setTableAlias(cTableAlias);
                }
                selectColumn.setAlias(alias);
                selectColumnList.add(selectColumn);
            }
        }
        return selectColumnList.toArray(new SelectColumn[0]);
    }


    private boolean isFunctionColumn(String functionStr) {
        String upperCase = functionStr.toUpperCase();
        if (isStartWithFunc(functionStr)) {
            if (upperCase.endsWith(")")) {
                return true;
            }
        }
        return false;
    }

    private boolean isStartWithFunc(String columnStr) {
        String upperCase = columnStr.toUpperCase();
        if (upperCase.startsWith(FunctionConstant.FUNC_COUNT + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_MAX + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_MIN + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_SUM + "(")
                || upperCase.startsWith(FunctionConstant.FUNC_AVG + "(")) {
            return true;
        }
        return false;
    }


    private SelectColumn parseFunction(TableColumnInfo columnInfo, String functionStr) {
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
                args = new String[1];
                String columnName0 = getFunctionArg(functionStr);
                String[] argColumns = columnName0.split("\\s+");
                String cName = argColumns.length > 1 ? argColumns[1] : argColumns[0];
                column = columnInfo.getColumn(cName);
                if (column == null) {
                    throw new SqlIllegalException("sql不合法，字段" + cName + "不存在");
                }
                args[0] = columnName0;
                break;
            case FunctionConstant.FUNC_MAX:
            case FunctionConstant.FUNC_MIN:
            case FunctionConstant.FUNC_SUM:
            case FunctionConstant.FUNC_AVG:
                args = new String[1];
                String columnName = getFunctionArg(functionStr);
                column = columnInfo.getColumn(columnName);
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
    private ConditionTree parseWhereCondition(ConditionTree conditionTree,
                                              Map<String, Column> columnMap,
                                              StartEndIndex bracketStartEnd,
                                              StartEndIndex subQueryStartEnd) {


        List<ConditionTree> childNodes = conditionTree.getChildNodes();
        String nextJoinType = ConditionConstant.AND;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }

            if(bracketStartEnd != null && bracketStartEnd.getStart() == currIndex) {
                currIndex++;
            }

            if(bracketStartEnd != null && bracketStartEnd.getEnd() == currIndex) {
                currIndex++;
                break;
            }

            if(bracketStartEnd != null && currIndex > bracketStartEnd.getEnd()) {
                break;
            }

            // 读到开始括号，创建一个条件树节点
            if (sqlCharArr[currIndex] == '(') {
                StartEndIndex startEnd = getNextBracketStartEnd();
                ConditionTree childNode = createChildNode2(nextJoinType);
                childNodes.add(childNode);
                parseWhereCondition(childNode, columnMap, startEnd, subQueryStartEnd);
            }


            // 预先读关键字，判断是AND还是OR
            String nextKeyWord = getNextKeyWordUnMove();
            if (ConditionConstant.AND.equals(nextKeyWord) || ConditionConstant.OR.equals(nextKeyWord)) {
                nextJoinType = nextKeyWord;
                // skip keyword
                getNextKeyWord();
                skipSpace();

                // 读到开始括号，创建一个条件树节点
                if (sqlCharArr[currIndex] == '(') {
                    StartEndIndex startEnd = getNextBracketStartEnd();
                    ConditionTree newNode = new ConditionTree();
                    newNode.setJoinType(nextJoinType);
                    newNode.setChildNodes(new ArrayList<>());
                    childNodes.add(newNode);
                    parseWhereCondition(newNode, columnMap, startEnd, subQueryStartEnd);
                    if (currIndex >= sqlCharArr.length) {
                        break;
                    }
                }
            }



            if (bracketStartEnd != null && currIndex == bracketStartEnd.getEnd()) {
                break;
            }

            // 条件开始
            if (sqlCharArr[currIndex] != ' ' && sqlCharArr[currIndex] != '(' && sqlCharArr[currIndex] != ')') {
                // 解析条件
                Condition condition = parseCondition(columnMap);
                // 构造条件树的叶子节点
                ConditionTree node = new ConditionTree();
                node.setLeaf(true);
                node.setJoinType(nextJoinType);
                node.setCondition(condition);
                childNodes.add(node);
            }

            if(readIf("AND") ||  readIf("OR")) {
                // 下一个关键字是AND或者OR，currIndex不进行移动。否则下次循环读下一个关键字(AND/OR)变成了"ND"和"R"。
                continue;
            }

            // 遇到limit、GROUP关键字结束
            if (readIf("LIMIT") ||  readIf("GROUP")) {
                break;
            }

            if (bracketStartEnd == null || currIndex != bracketStartEnd.getEnd()) {
                if(subQueryStartEnd != null && subQueryStartEnd.getEnd() == currIndex) {
                    currIndex++;
                }
                break;
            }



            currIndex++;
        }
        return conditionTree;
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
                // 读一个条件表达式
                left = readLeftExpression(columnMap);
                if(left instanceof ColumnExpression) {
                    left = readRightExpression(columnMap, left);
                }
                String andOrType = getNextKeyWordUnMove();
                if ("AND".equals(andOrType) || "OR".equals(andOrType)) {
                    getNextKeyWord();
                    Expression right = readCondition(columnMap);
                    left = new ConditionAndOr2(andOrType, left, right);
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
            if ("AND".equals(andOrType) || "OR".equals(andOrType)) {
                getNextKeyWord();
                Expression right = readCondition(columnMap);
                l = new ConditionAndOr2(andOrType, l, right);
            }
            break;
        } else {
            l = parseConditionColumn(columnMap);
            break;
        }
       }
        return l;
    }


    public Expression readRightCondition(Map<String, Column> columnMap, Expression l) {
        String nextKeyWord = getNextKeyWordUnMove();
        if ("AND".equals(nextKeyWord) || "OR".equals(nextKeyWord)) {
            getNextKeyWord();
            Expression right = readCondition(columnMap);
            l = new ConditionAndOr2(nextKeyWord, l, right);
        }
        return l;
    }



    public Expression parseConditionColumn(Map<String, Column> columnMap) {
        skipSpace();
        int start = currIndex;
        Expression columnExpression = null;
        Column column = null;
        while (true) {
            if (currIndex >= sqlCharArr.length) {
                break;
            }
            // flag=1，当前为字段字段名
            if ((sqlCharArr[currIndex] == ' ' || sqlCharArr[currIndex] == '=' || sqlCharArr[currIndex] == '!' || sqlCharArr[currIndex] == '<')) {
                String columnName = originalSql.substring(start, currIndex);
                if (columnName.startsWith("'") && columnName.endsWith("'")) {
                    column = new Column(null, ColumnTypeEnum.VARCHAR.getColumnType(), 0, 0);
                    column.setValue(columnName.substring(1, columnName.length() - 1));
                } else if (isNumericString(columnName)) {
                    column = new Column(null, ColumnTypeEnum.VARCHAR.getColumnType(), 0, 0);
                    column.setValue(columnName);
                } else {
                    column = columnMap.get(columnName);
                    if (column == null) {
                        throw new SqlIllegalException("sql语法有误，字段不存在：" + columnName);
                    }
                }
                column = column.copy();
                columnExpression = new ColumnExpression(column);
                skipSpace();
                break;
            }
            currIndex++;
        }

        if(columnExpression == null) {
            throw new DbException("解析sql发生异常，sql=" + originalSql);
        }


        return columnExpression;
    }

    private Expression readRightExpression(Map<String, Column> columnMap, Expression left) {
        Expression condition = null;
        String operator = getNextKeyWord();
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
                    throw new DbException("不支持EXISTS查询");
                }
                break;
            case "IS":
                String word1 = getNextKeyWord();
                if ("NULL".equals(word1)) {
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


    private SingleComparison parseSingleComparison(Map<String,Column> columnMap, Expression left, String operator){
        SingleComparison condition = null;
        String nextValue = getNextOriginalWordUnMove();

        Expression right = null;
        if ("(".equals(nextValue) || "(SELECT".equals(nextValue)) {
            StartEndIndex subStartEnd = getNextBracketStartEnd();
            currIndex++;
            assertNextKeywordIs(SELECT);
            // 解析子查询
            Query subQuery = parseQuery(subStartEnd);
            right = new SubQueryValue((ColumnExpression) left, subQuery);
        } else {
            Column rightColumn = columnMap.get(nextValue);
            // attribute = value
            if (rightColumn == null) {
                String v = parseSimpleConditionValue();
                // 转换值为对应类型对象
                Object obj = getTypeValueObj(left, v);
                right = new ConstantValue(obj);
            } else {
                // attribute = attribute
                right = parseConditionColumn(columnMap);
            }
        }
        condition = new SingleComparison(operator, left, right);

        return condition;
    }

    private Object getTypeValueObj(Expression left, String value){
        Object obj = null;
        if(left instanceof ColumnExpression) {
            ColumnExpression c = (ColumnExpression) left;
            Column column = c.getColumn();
            obj = TypeConvertUtil.convertValueType(value, column.getColumnType());
        } else {
            obj = value;
        }
        return obj;
    }



    private boolean readIf(String str) {
        String next = getNextKeyWordUnMove();
        if (str.equals(next)) {
            getNextKeyWord();
            return true;
        }
        return false;
    }


    private ConditionTree createChildNode2(String joinType) {
        ConditionTree newNode = new ConditionTree();
        newNode.setJoinType(joinType);
        newNode.setChildNodes(new ArrayList<>());
        return newNode;
    }

    private Condition parseJoinCondition(Map<String, Column> columnMap) {
        Condition condition = parseCondition(columnMap);
        return condition;
    }


    private Condition parseCondition(Map<String,Column> columnMap) {
        skipSpace();
        int start = currIndex;
        Condition condition = null;
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
                if(columnName.startsWith("'") && columnName.endsWith("'")) {
                    column = new Column(null, ColumnTypeEnum.VARCHAR.getColumnType(), 0, 0);
                    column.setValue(columnName.substring(1, columnName.length() - 1));
                } else if(isNumericString(columnName)) {
                    column = new Column(null, ColumnTypeEnum.VARCHAR.getColumnType(), 0, 0);
                    column.setValue(columnName);
                }else {
                    column = columnMap.get(columnName);
                    if (column == null) {
                        throw new SqlIllegalException("sql语法有误，字段不存在：" + columnName);
                    }
                }
                column = column.copy();
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

                // 没有字段名情况，场景条件: 1 = 1
                if(column.getColumnName() == null) {
                    switch (nextKeyWord) {
                        case OperatorConstant.EQUAL:
                        case OperatorConstant.NOT_EQUAL_1:
                        case OperatorConstant.NOT_EQUAL_2:
                            boolean isEq = OperatorConstant.EQUAL.equals(nextKeyWord) ? true : false;
                            String left = (String) column.getValue();
                            String right = parseSimpleConditionValue(start);
                            condition = new ConditionEqOrNq2(left, right, isEq);
                            break;
                        default:
                            throw new SqlIllegalException("sql语法有误");
                    }

                    break;
                }

                // 有字段名情况，例如a = '1'
                switch (nextKeyWord) {
                    case OperatorConstant.EQUAL:
                    case OperatorConstant.NOT_EQUAL_1:
                    case OperatorConstant.NOT_EQUAL_2:
                        boolean isEq = OperatorConstant.EQUAL.equals(nextKeyWord) ? true : false;
                        String nextValue = getNextOriginalWordUnMove();
                        if ("(".equals(nextValue) || "(SELECT".equals(nextValue)) {
                            StartEndIndex subStartEnd = getNextBracketStartEnd();
                            currIndex++;
                            assertNextKeywordIs(SELECT);
                            // 解析子查询
                            Query subQuery = parseQuery(subStartEnd);
                            ConditionEqOrNq eqSubQuery = new ConditionEqOrNq(column, null, isEq);
                            eqSubQuery.setEqSubQuery(true);
                            eqSubQuery.setSubQuery(subQuery);
                            condition = eqSubQuery;
                        } else {
                            Column rightColumn = columnMap.get(nextValue);
                            if (rightColumn == null) {
                                // attribute = value
                                values = parseConditionValues(start, OperatorConstant.EQUAL);
                                condition = new ConditionEqOrNq(column, values.get(0), isEq);
                            } else {
                                // attribute = attribute
                                getNextOriginalWord();
                                condition = new ConditionLeftRight(column, rightColumn);
                            }
                        }
                        break;
                    case OperatorConstant.IN:
                        condition = getInOrNotInCondition(column, true);
                        break;
                    case OperatorConstant.EXISTS:
                    case OperatorConstant.LIKE:
                        values = parseConditionValues(start, OperatorConstant.LIKE);
                        condition = new ConditionLikeOrNot(column, values.get(0), true);
                        break;
                    case "NOT":
                        skipSpace();
                        String word0 = getNextKeyWord();
                        if ("IN".equals(word0)) {
                            condition = getInOrNotInCondition(column, false);
                        } else if ("LIKE".equals(word0)) {
                            start = currIndex;
                            skipSpace();
                            String v = parseSimpleConditionValue(start);
                            condition = new ConditionLikeOrNot(column, v, false);
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
                    case OperatorConstant.LESS_THAN:
                    case OperatorConstant.LESS_THAN_OR_EQUAL:
                        String nextValue1 = getNextOriginalWordUnMove();
                        if ("(".equals(nextValue1) || "(SELECT".equals(nextValue1)) {
                            StartEndIndex subStartEnd = getNextBracketStartEnd();
                            currIndex++;
                            assertNextKeywordIs(SELECT);
                            // 解析子查询
                            Query subQuery = parseQuery(subStartEnd);
                            ConditionRange conditionRange = new ConditionRange(column, null, null, nextKeyWord);
                            conditionRange.setHasSubQuery(true);
                            conditionRange.setUpperSubQuery(subQuery);
                            condition = conditionRange;
                        } else {
                            String value1 = parseSimpleConditionValue(start);
                            ConditionRange conditionRange = new ConditionRange(column, null, value1, nextKeyWord);
                            condition = conditionRange;
                        }
                        break;
                    case OperatorConstant.GREATER_THAN:
                    case OperatorConstant.GREATER_THAN_OR_EQUAL:
                        String nextValue2 = getNextOriginalWordUnMove();
                        if ("(".equals(nextValue2) || "(SELECT".equals(nextValue2)) {
                            StartEndIndex subStartEnd = getNextBracketStartEnd();
                            currIndex++;
                            assertNextKeywordIs(SELECT);
                            // 解析子查询
                            Query subQuery = parseQuery(subStartEnd);
                            ConditionRange conditionRange = new ConditionRange(column, null, null, nextKeyWord);
                            conditionRange.setHasSubQuery(true);
                            conditionRange.setLowerSubQuery(subQuery);
                            condition = conditionRange;
                        } else {
                            String value2 = parseSimpleConditionValue(start);
                            condition = new ConditionRange(column, value2, null, nextKeyWord);
                        }
                        break;
                    case OperatorConstant.BETWEEN:
                        String lowerLimit = parseSimpleConditionValue(start);
                        assertNextKeywordIs("AND");
                        skipSpace();
                        String upperLimit = parseSimpleConditionValue(currIndex);
                        condition = new ConditionRange(column, lowerLimit, upperLimit, nextKeyWord);
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
        if (!(inValueStr.startsWith("SELECT") || inValueStr.startsWith("select"))) {
            String[] split = inValueStr.split(",");
            for (String v : split) {
                String value = v.trim();
                if (v.startsWith("'") && value.endsWith("'") && value.length() > 1) {
                    value = value.substring(1, value.length() - 1);
                }
                // 转换值为对应类型对象
                Object obj = null;
                if(left instanceof ColumnExpression) {
                    ColumnExpression c = (ColumnExpression) left;
                    Column column = c.getColumn();
                    obj = TypeConvertUtil.convertValueType(value, column.getColumnType());
                } else {
                    obj = value;
                }
                values.add(new ConstantValue(obj));
            }
            currIndex = startEnd.getEnd();
            condition = new ConditionIn(isIn, left, values);
        } else {
            currIndex = startEnd.getStart();
            getNextKeyWord();
            Query query = parseQuery(startEnd);
            condition = new ConditionInSubQuery(isIn, left, query);
        }
        return condition;
    }

    private ConditionInOrNot getInOrNotInCondition(Column column, boolean isIn) {
        ConditionInOrNot condition = null;
        List<String> values = new ArrayList<>();
        StartEndIndex startEnd = getNextBracketStartEnd();
        String inValueStr = originalSql.substring(startEnd.getStart() + 1, startEnd.getEnd());
        if (!(inValueStr.startsWith("SELECT") || inValueStr.startsWith("select"))) {
            String[] split = inValueStr.split(",");
            for (String v : split) {
                String value = v.trim();
                if (v.startsWith("'") && value.endsWith("'") && value.length() > 1) {
                    value = v.substring(1, v.length() - 1);
                }
                values.add(value);
            }
            currIndex = startEnd.getEnd();
            condition = new ConditionInOrNot(column, values, isIn);
        } else {
            currIndex = startEnd.getStart();
            String nextKeyWord = getNextKeyWord();
            Query query = parseQuery(startEnd);
            condition = new ConditionInOrNot(column, query, isIn);
        }
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


    private String parseSimpleConditionValue() {
        String value = null;
        // 标记字符串 "'" 符号是否打开状态
        boolean strOpen = false;
        int start = currIndex;
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
    private InsertCommand getInsertCommand() {

        skipSpace();

        String word = getNextKeyWord();
        if (!"INTO".equals(word)) {
            throw new SqlIllegalException("sql语法有误," + word);
        }

        // 解析tableName
        String tableName = parseTableNameOrIndexName();

        // ==== 读取字段 ====
        StartEndIndex columnBracket = getNextBracketStartEnd();
        String columnStr = originalSql.substring(columnBracket.getStart() + 1, columnBracket.getEnd());
        String[] columnNameList = columnStr.split(",");
        currIndex =  columnBracket.getEnd() + 1;

        // ==== 读字段值 ===
        skipSpace();
        String valueKeyWord = getNextKeyWord();
        if (!"VALUE".equals(valueKeyWord) && !"VALUES".equals(valueKeyWord)) {
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
        Column[] dataColumns = new Column[columns.length];

        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i].copy();
            String value = columnValueMap.get(column.getColumnName());
            setColumnValue(column, value);
            dataColumns[i] = column;
        }

        OperateTableInfo tableInfo = getOperateTableInfo(tableName, columns, null);
        return new InsertCommand(tableInfo, dataColumns);
    }


    private void setColumnValue(Column column, String value) {

        // TODO 默认空值，以后判断字段是否非空做校验
        if(value == null) {
            value = "null";
        }

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
        skipSpace();
        String tableName = parseTableNameOrIndexName();

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

        currIndex = bracketStartEnd.getEnd() + 1;

        String engineType = null;
        String nextOriginalWord = getNextOriginalWord();
        if(nextOriginalWord.startsWith("ENGINE=")) {
            engineType = nextOriginalWord.substring("ENGINE=".length());
        }
        if(engineType == null) {
            engineType = CommonConstant.ENGINE_TYPE_YU;
        }
        // 构造创建表命令
        CreateTableCommand command = new CreateTableCommand();
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
