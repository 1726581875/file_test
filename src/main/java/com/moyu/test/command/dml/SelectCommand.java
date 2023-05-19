package com.moyu.test.command.dml;

import com.moyu.test.command.AbstractCommand;
import com.moyu.test.command.QueryResult;
import com.moyu.test.command.condition.ConditionComparator;
import com.moyu.test.command.condition.ConditionTree;
import com.moyu.test.constant.ConditionConstant;
import com.moyu.test.exception.SqlIllegalException;
import com.moyu.test.store.data.DataChunk;
import com.moyu.test.store.data.DataChunkStore;
import com.moyu.test.store.data.RowData;
import com.moyu.test.store.metadata.obj.Column;
import com.moyu.test.util.PathUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/17
 */
public class SelectCommand extends AbstractCommand {

    private static final String FILE_PATH = PathUtil.getBaseDirPath();

    private String tableName;

    private Column[] columns;

    private ConditionTree conditionTree;

    private QueryResult queryResult;


    public SelectCommand(String tableName, Column[] columns, ConditionTree conditionTree) {
        this.tableName = tableName;
        this.columns = columns;
        this.conditionTree = conditionTree;
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
        for (Column column : columns) {
            int length = column.getColumnName().length();
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

        // 表头
        String tableHeaderStr = "";
        for (Column column : columns) {
            String value = column.getColumnName();
            tableHeaderStr = tableHeaderStr + " | " + value;
        }
        stringBuilder.append(tableHeaderStr + " | " + "\n");

        // 分界线
        appendLine(stringBuilder);

        Column[] resultColumns = queryResult.getColumns();
        // 值
        List<Object[]> resultRows = queryResult.getResultRows();
        for (int i = 0; i < resultRows.size(); i++) {
            Object[] rowValues = resultRows.get(i);
            String rowStr = "";
            for (int j = 0; j < rowValues.length; j++) {
                // 按照条件过滤
                Object value = rowValues[j];
                String valueStr = (value == null ? "" : value.toString());
                int length = resultColumns[j].getColumnName().length();
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



    public QueryResult execQuery() {
        QueryResult result = new QueryResult();
        result.setColumns(columns);
        result.setResultRows(new ArrayList<>());
        DataChunkStore dataChunkStore = null;
        try {
            String fileFullPath = FILE_PATH + tableName + ".d";
            dataChunkStore = new DataChunkStore(fileFullPath);
            int dataChunkNum = dataChunkStore.getDataChunkNum();
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
                    if(conditionTree == null) {
                        result.addRow(columnData);
                    } else {
                        boolean compareResult = ConditionComparator.analyzeConditionTree(conditionTree, columnData);
                        if(compareResult) {
                            result.addRow(columnData);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            dataChunkStore.close();
        }

        this.queryResult = result;
        return this.queryResult;
    }




    public QueryResult getQueryResult() {
        return queryResult;
    }
}
