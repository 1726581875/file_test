package com.moyu.test.command.condition;

import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/5/18
 */
public class ConditionTree {
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
    private List<ConditionTree> childNodes;
    /**
     * 叶子节点不为null
     */
    private Condition condition;
    /**
     * 是否叶子节点
     */
    private boolean isLeaf;


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

    public List<ConditionTree> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<ConditionTree> childNodes) {
        this.childNodes = childNodes;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }
}
