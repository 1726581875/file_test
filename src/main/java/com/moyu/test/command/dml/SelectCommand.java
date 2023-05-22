package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.condition.ConditionComparator;
import com.moyu.test.command.condition.ConditionTree;
import com.moyu.test.command.dml.function.*;
import com.moyu.test.constant.DbColumnTypeConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.store.metadata.obj.SelectColumn;
import com.moyu.test.util.PathUtil;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class SelectCommand extends AbstractCommand {

    private Integer databaseId;

    private String tableName;

    private Column[] columns;

    private SelectColumn[] selectColumns;

    private ConditionTree conditionTree;

    private Integer limit;

    private Integer offset = 0;

    private QueryResult queryResult;


    public SelectCommand(Integer databaseId,
                         String tableName,
                         Column[] columns,
                         SelectColumn[] selectColumns) {
        this.databaseId = databaseId;
        this.tableName = tableName;
        this.columns = columns;
        this.selectColumns = selectColumns;
    }

    @Override
    public String execute() {
        long queryStartTime = System.currentTimeMillis();
        // 执行查询
        QueryResult queryResult = execQuery();
        long queryEndTime = System.currentTimeMillis();

        // 解析结果，打印拼接结果字符串
        return getResultPrintStr(queryResult, queryStartTime, queryEndTime);
    }

    private void appendLine(StringBuilder stringBuilder) {
        stringBuilder.append(" ");
        for (SelectColumn column : selectColumns) {
            int length = column.getSelectColumnName().length();
            while (length > 0) {
                stringBuilder.append("--");
                length--;
            }
        }
        stringBuilder.append("\n");
    }

    private String getStr(char c, int num) {
        StringBuilder stringBuilder = new StringBuilder();
        while (num > 0) {
            stringBuilder.append(c);
            num--;
        }
        return stringBuilder.toString();
    }


    private String getResultPrintStr(QueryResult queryResult, long queryStartTime,long queryEndTime) {
        // 解析结果，打印拼接结果字符串
        StringBuilder stringBuilder = new StringBuilder();
        // 分界线
        appendLine(stringBuilder);


        SelectColumn[] selectColumns = queryResult.getSelectColumns();
        // 表头
        String tableHeaderStr = "";
        for (SelectColumn column : selectColumns) {
            String value = column.getSelectColumnName();
            tableHeaderStr = tableHeaderStr + " | " + value;
        }
        stringBuilder.append(tableHeaderStr + " | " + "\n");

        // 分界线
        appendLine(stringBuilder);

        SelectColumn[] resultColumns = queryResult.getSelectColumns();
        // 值
        List<Object[]> resultRows = queryResult.getResultRows();
        for (int i = 0; i < resultRows.size(); i++) {
            Object[] rowValues = resultRows.get(i);
            String rowStr = "";
            for (int j = 0; j < rowValues.length; j++) {
                // 按照条件过滤
                Object value = rowValues[j];
                String valueStr = (value == null ? "" : valueToString(value));
                int length = resultColumns[j].getSelectColumnName().length();
                if(length > valueStr.length()) {
                    int spaceNum = (length - valueStr.length()) / 2;
                    rowStr = rowStr + " | "+ getStr(' ',spaceNum) + valueStr + getStr(' ',spaceNum);
                } else {
                    rowStr = rowStr + " | " + valueStr;
                }
            }
            stringBuilder.append(rowStr + " | " + "\n");
        }

        stringBuilder.append("查询结果行数:" +  resultRows.size() + ", 耗时:" + (queryEndTime - queryStartTime)  + "ms");

        return stringBuilder.toString();
    }

    private String valueToString(Object value) {
        if (value instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return dateFormat.format((Date) value);
        } else {
            return value.toString();
        }
    }



    public QueryResult execQuery() {
        QueryResult result = new QueryResult();
        result.setSelectColumns(selectColumns);
        result.setResultRows(new ArrayList<>());
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = PathUtil.getDataFilePath(this.databaseId, this.tableName);
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
            List<Column[]> dataList = null;
            // select后面是统计函数
            if(isFunction()) {
                dataList = getFunctionResultList(dataChunkNum, dataChunkStore);
            } else {
                // select后面是字段
                dataList = getColumnDataList(dataChunkNum, dataChunkStore);
            }
            result.addAll(dataList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }

        this.queryResult = result;
        return this.queryResult;
    }

    /**
     * TODO当前只能做到SELECT后面全部是函数或者全部是字段
     * @return
     */
    private boolean isFunction() {
        // 如果第一个是函数，后面必须都是函数
        if (selectColumns[0].getFunctionName() != null) {
            for (SelectColumn c : selectColumns) {
                if(c.getFunctionName() == null) {
                    throw new SqlIllegalException("sql语法有误");
                }
            }
            return true;
        }
        // 否则就是查询zid
        return false;
    }


    private List<Column[]> getColumnDataList(int dataChunkNum, DataChunkStore dataChunkStore) {
        List<Column[]> dataList = new ArrayList<>();

        int currIndex = 0;
        // 遍历数据块
        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            // 获取数据块包含的数据行
            List<RowData> dataRowList = chunk.getDataRowList();
            if (dataRowList == null || dataRowList.size() == 0) {
                continue;
            }
            for (int j = 0; j < dataRowList.size(); j++) {
                RowData rowData = dataRowList.get(j);
                Column[] columnData = rowData.getColumnData(columns);
                // 按照条件过滤
                if (conditionTree == null) {
                    Column[] filterColumns = filterColumns(columnData);
                    // 判断是否符合limit、offset
                    if(isLimitRow(currIndex)) {
                        dataList.add(filterColumns);
                    }
                    currIndex++;
                } else {
                    boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
                    if (compareResult) {
                        Column[] filterColumns = filterColumns(columnData);
                        // 判断是否符合limit、offset
                        if(isLimitRow(currIndex)) {
                            dataList.add(filterColumns);
                        }
                        currIndex++;
                    }
                }
            }
        }

        return dataList;
    }


    /**
     * 是否是符合offset和limit条件的行
     * @param currIndex
     * @return
     */
    private boolean isLimitRow(int currIndex) {
        if (offset != null && limit != null) {
            int beginIndex = offset;
            int endIndex = offset + limit - 1;
            if (currIndex < beginIndex || currIndex > endIndex) {
                return false;
            }
        }
        return true;
    }


    private List<Column[]> getFunctionResultList(int dataChunkNum, DataChunkStore dataChunkStore) {

        List<Column[]> dataList = new ArrayList<>();


        // 1、初始化所有统计函数对象
        List<StatFunction> statFunctions = new ArrayList<>(selectColumns.length);
        for (SelectColumn selectColumn : selectColumns) {
            String functionName = selectColumn.getFunctionName();
            if(functionName == null) {
                throw new SqlIllegalException("sql语法错误，functionName应当不为空");
            }

            String columnName = selectColumn.getArgs()[0];
            if(columnName == null) {
                throw new SqlIllegalException("sql语法错误，column应当不为空");
            }

            switch (functionName) {
                case "count":
                    statFunctions.add(new CountFunction(columnName));
                    break;
                case "sum":
                    statFunctions.add(new SumFunction(columnName));
                    break;
                case "min":
                    statFunctions.add(new MinFunction(columnName));
                    break;
                case "max":
                    statFunctions.add(new MaxFunction(columnName));
                    break;
                default:
                    throw new SqlIllegalException("sql语法错误，不支持该函数：" + functionName);
            }
        }


        // 2、遍历数据，执行计算函数
        for (int i = 0; i < dataChunkNum; i++) {
            DataChunk chunk = dataChunkStore.getChunk(i);
            if (chunk == null) {
                break;
            }
            // 获取数据块包含的数据行
            List<RowData> dataRowList = chunk.getDataRowList();
            if (dataRowList == null || dataRowList.size() == 0) {
                continue;
            }
            for (int j = 0; j < dataRowList.size(); j++) {
                RowData rowData = dataRowList.get(j);
                Column[] columnData = rowData.getColumnData(columns);
                // 没有where条件
                if(conditionTree == null) {
                    // 进入计算函数
                    for (StatFunction statFunction : statFunctions) {
                        statFunction.stat(columnData);
                    }
                    // 按where条件过滤
                } else {
                    boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
                    if(compareResult) {
                        // 进入计算函数
                        for (StatFunction statFunction : statFunctions) {
                            statFunction.stat(columnData);
                        }
                    }
                }
            }
        }

        // 汇总执行结果
        Column[] resultColumns = new Column[statFunctions.size()];
        for (int i = 0; i < statFunctions.size(); i++) {
            StatFunction statFunction = statFunctions.get(i);

            Long statResult = statFunction.getValue();
            Column resultColumn = null;
            Column c = selectColumns[i].getColumn();
            // 日期类型
            if(c != null && c.getColumnType() == DbColumnTypeConstant.TIMESTAMP) {
                resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.TIMESTAMP, i, 8);
                resultColumn.setValue(statResult == null ? null : new Date(statResult));
            } else {
                // 数字类型
                resultColumn = new Column(statFunction.getColumnName(), DbColumnTypeConstant.INT_8, i, 8);
                resultColumn.setValue(statResult);
            }

            resultColumns[i] = resultColumn;
        }
        dataList.add(resultColumns);


        return dataList;
    }



    private Column[] filterColumns(Column[] columnData) {
        Column[] resultColumns = new Column[selectColumns.length];
        for (int i = 0; i < selectColumns.length; i++) {
            if(selectColumns[i].getColumn() != null) {
                Column c = selectColumns[i].getColumn();
                resultColumns[i] = columnData[c.getColumnIndex()];
            }
        }
        return resultColumns;
    }





    public QueryResult getQueryResult() {
        return queryResult;
    }

    public ConditionTree getConditionTree() {
        return conditionTree;
    }

    public void setConditionTree(ConditionTree conditionTree) {
        this.conditionTree = conditionTree;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }
}
