package com.moyu.test.command.dml.plan;

import com.moyu.test.command.QueryResult;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/5
 */
public class QueryPlan {
    /**
     * 总代价
     */
    private int cost;
    /**
     * 是否叶子节点
     */
    private boolean isLeaf;

    /**
     * 子查询计划
     */
    private List<QueryPlan> subPlans;
    /**
     * 当前节点查询结果
     */
    private QueryResult queryResult;
}
