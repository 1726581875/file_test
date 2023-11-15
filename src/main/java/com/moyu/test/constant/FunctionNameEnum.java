package com.moyu.test.constant;

/**
 * @author xiaomingzhang
 * @date 2023/11/14
 */
public enum FunctionNameEnum {

    COUNT(FunctionConstant.FUNC_COUNT),
    MAX(FunctionConstant.FUNC_MAX),
    MIN(FunctionConstant.FUNC_MIN),
    SUM(FunctionConstant.FUNC_SUM),
    AVG(FunctionConstant.FUNC_AVG),
    UUID(FunctionConstant.FUNC_UUID),
    NOW(FunctionConstant.FUNC_NOW),
    UNIX_TIMESTAMP(FunctionConstant.UNIX_TIMESTAMP),
    FROM_UNIXTIME(FunctionConstant.FROM_UNIXTIME)
    ;
    private String name;


    FunctionNameEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean isFunction(String columnStr) {
        String upperCase = columnStr.toUpperCase();
        for (FunctionNameEnum func : FunctionNameEnum.values()) {
            if(upperCase.startsWith(func.getName() + "(") && upperCase.endsWith(")")) {
                return true;
            }
        }
        return false;
    }



}
