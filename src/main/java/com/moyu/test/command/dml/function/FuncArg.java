package com.moyu.test.command.dml.function;

/**
 * @author xiaomingzhang
 * @date 2023/10/24
 * 函数参数
 */
public class FuncArg {
    /**
     * 参数下标
     */
    private int idx;
    /**
     * 参数类型
     */
    private String argType;
    /**
     * 参数值
     */
    private Object argValue;

    public FuncArg(int idx, String argType, Object argValue) {
        this.idx = idx;
        this.argType = argType;
        this.argValue = argValue;
    }

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public String getArgType() {
        return argType;
    }

    public void setArgType(String argType) {
        this.argType = argType;
    }

    public Object getArgValue() {
        return argValue;
    }

    public void setArgValue(Object argValue) {
        this.argValue = argValue;
    }
}
