package com.moyu.test.util;

/**
 * @author xiaomingzhang
 * @date 2023/9/30
 */
public class SqlParserUtil {


    /**
     * 获取不包含两侧引号的字符串
     * 如`name` -> name
     * @param str
     * @return
     */
    public static String getUnquotedStr(String str){
        if((str.startsWith("`") && str.endsWith("`"))
                || (str.startsWith("\"") && str.endsWith("\""))) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }


}
