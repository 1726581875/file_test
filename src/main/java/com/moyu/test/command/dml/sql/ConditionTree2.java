package com.moyu.test.command.dml.sql;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/6/10
 */
public class ConditionTree2 {

    /**
     * 非叶子节点不为空，AND/OR
     */
    private String joinType;
    /**
     * 条件结果
     */
    private boolean result;
    /**
     * 子节点
     */
    private List<ConditionTree2> childNodes;
    /**
     * 叶子节点不为null
     */
    private Condition2 condition;
    /**
     * 是否叶子节点
     */
    private boolean isLeaf;


    public ConditionTree2(){}

    public ConditionTree2(String joinType, List<ConditionTree2> childNodes, boolean isLeaf) {
        this.joinType = joinType;
        this.childNodes = childNodes;
        this.isLeaf = isLeaf;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public List<ConditionTree2> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<ConditionTree2> childNodes) {
        this.childNodes = childNodes;
    }

    public Condition2 getCondition() {
        return condition;
    }

    public void setCondition(Condition2 condition) {
        this.condition = condition;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

}
