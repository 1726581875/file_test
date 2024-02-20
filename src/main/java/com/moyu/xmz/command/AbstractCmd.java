package com.moyu.xmz.command;

import com.moyu.xmz.command.dml.sql.Parameter;
import com.moyu.xmz.common.exception.ExceptionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/5/6
 */
public abstract class AbstractCmd implements Command {

    protected final static String RESULT_OK = "ok";
    protected final static String RESULT_ERROR = "error";


    private List<Parameter> parameters = new ArrayList<>();


    @Override
    public QueryResult exec() {
        return null;
    }


    public List<Parameter> getParameters() {
        return parameters;
    }

    public void addParameters(List<Parameter> parameters) {
        this.parameters.addAll(parameters);
    }

    public void setParameterValues(List<Parameter> parameterValues) {
        if(parameters.size() != parameterValues.size()) {
            ExceptionUtil.throwSqlQueryException("参数数量不一致,传入参数数量为:{},可接受参数数量为:{}",parameterValues.size(), parameters.size());
        }

        Map<Integer, Parameter> paramValueMap = new HashMap<>(parameterValues.size());
        for (Parameter p : parameterValues) {
            paramValueMap.put(p.getIndex(), p);
        }

        for (int i = 0; i < parameters.size(); i++) {
            Parameter parameter = parameters.get(i);
            Parameter p = paramValueMap.get(parameter.getIndex());
            if(p == null) {
                ExceptionUtil.throwSqlQueryException("缺少参数，参数下标{}", parameter.getIndex());
            }
            parameter.setValue(p.getValue());
        }
    }
}
