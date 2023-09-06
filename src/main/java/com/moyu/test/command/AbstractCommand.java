package com.moyu.test.command;

import com.moyu.test.command.dml.sql.Parameter;
import com.moyu.test.exception.DbException;
import com.moyu.test.store.metadata.obj.SelectColumn;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public abstract class AbstractCommand implements Command {


    private List<Parameter> parameters = new ArrayList<>();


    @Override
    public String[] exec() {
        String result = execute();
        String[] strings = new String[1];
        strings[0] = result;
        return strings;
    }

    @Override
    public QueryResult execCommand() {
        String executeResult = execute();
        SelectColumn selectColumn = new SelectColumn(null, "执行结果", null, null);
        QueryResult queryResult = new QueryResult();
        queryResult.setSelectColumns(new SelectColumn[]{selectColumn});
        queryResult.addRow(new Object[]{executeResult});
        return queryResult;
    }

    /**
     * 执行命令
     *
     * @param
     * @return
     */
    public abstract String execute();


    @Override
    public void reUse() {

    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void addParameters(List<Parameter> parameters) {
        this.parameters.addAll(parameters);
    }

    public void setParameterValues(List<Parameter> parameterValues) {
        if(parameters.size() != parameterValues.size()) {
            throw new DbException("参数数量不一致,传入参数数量为:" + parameterValues.size() + ",可接受参数数量为:" + parameters.size());
        }
        for (int i = 0; i < parameters.size(); i++) {
            parameters.get(i).setValue(parameterValues.get(i).getValue());
        }
    }
}
