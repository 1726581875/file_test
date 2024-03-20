package com.moyu.xmz.common.util;

/**
 * @author xiaomingzhang
 * @date 2023/9/30
 */
public class SqlParserUtils {


    /**
     * 获取不包含两侧引号的字符串
     * 如`name` -> name
     * @param str
     * @return
     */
    public static String getUnquotedStr(String str){
        if(isContainQuotes(str)) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }

    public static boolean isContainQuotes(String str) {
        return ((str.startsWith("`") && str.endsWith("`"))
                || (str.startsWith("\"") && str.endsWith("\""))
                || (str.startsWith("'") && str.endsWith("'")));
    }


}
