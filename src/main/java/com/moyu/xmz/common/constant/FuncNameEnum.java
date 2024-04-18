package com.moyu.xmz.common.constant;

/**
 * @author xiaomingzhang
 * @date 2023/11/14
 */
public enum FuncNameEnum {

    COUNT(FuncConstant.FUNC_COUNT),
    MAX(FuncConstant.FUNC_MAX),
    MIN(FuncConstant.FUNC_MIN),
    SUM(FuncConstant.FUNC_SUM),
    AVG(FuncConstant.FUNC_AVG),
    UUID(FuncConstant.FUNC_UUID),
    NOW(FuncConstant.FUNC_NOW),
    NOW_TIMESTAMP(FuncConstant.NOW_TIMESTAMP),
    UNIX_TIMESTAMP(FuncConstant.UNIX_TIMESTAMP),
    TO_TIMESTAMP(FuncConstant.TO_TIMESTAMP),
    FROM_UNIXTIME(FuncConstant.FROM_UNIXTIME)
    ;
    private String name;


    FuncNameEnum(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static boolean isFunction(String columnStr) {
        String upperCase = columnStr.toUpperCase();
        for (FuncNameEnum func : FuncNameEnum.values()) {
            if(upperCase.startsWith(func.getName() + "(") && upperCase.endsWith(")")) {
                return true;
            }
        }
        return false;
    }



}
