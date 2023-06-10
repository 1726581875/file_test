package com.moyu.test.command.dml.sql;

import com.moyu.test.command.dml.condition.ConditionTree;
import com.moyu.test.store.metadata.obj.Column;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 * 一个查询
 */
public class Query {

    /**
     * select
     */
    private Column[] selectColumns;

    private TableFilter mainTable;

    private ConditionTree conditionTree;


}
