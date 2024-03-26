package com.moyu.xmz.common.util;

import com.moyu.xmz.common.exception.ExceptionUtil;

import java.util.ArrayList;
import java.util.List;

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

    /**
     * 按逗号切分为一个个字段
     * 输入: id int,name varchar(64), state char(10), login_state tinyint
     * 输出:['id int,name varchar(64)','state char(10)','login_state tinyint']
     * @param inputStr
     * @param splitChar
     * @return
     */
    public static String[] splitQuotMarksByChar(String inputStr, char splitChar) {
        List<String> columnStrList = new ArrayList<>();
        char[] charArray = inputStr.toCharArray();
        int curIdx = 0;
        int columnStart = 0;
        Character curQuotationMarks = null;
        while (curIdx < charArray.length) {
            char c = charArray[curIdx];
            // 引号开始
            if (curQuotationMarks == null && (c == '"' || c == '\'')) {
                curQuotationMarks = c;
            } else if (curQuotationMarks != null && c == curQuotationMarks) {
                // 引号结束
                curQuotationMarks = null;
            }
            // 不是在引号里面的逗号作为字段分割
            if(c == splitChar && curQuotationMarks == null) {
                if(curIdx > columnStart) {
                    String columnDefineStr = inputStr.substring(columnStart, curIdx).trim();
                    if(columnDefineStr.length() > 0) {
                        columnStrList.add(columnDefineStr);
                    } else {
                        if(splitChar != ' ') {
                            ExceptionUtil.throwSqlExecutionException("sql语法有误，在{}符号附近", splitChar);
                        }
                    }
                    columnStart = curIdx + 1;
                } else {
                    ExceptionUtil.throwSqlExecutionException("sql语法有误，在{}符号附近", splitChar);
                }
            }

            if (curIdx == charArray.length - 1 && columnStart < charArray.length) {
                String columnDefineStr = inputStr.substring(columnStart, charArray.length).trim();
                if (columnDefineStr.length() > 0) {
                    columnStrList.add(columnDefineStr);
                }
            }
            curIdx++;
        }

        return columnStrList.toArray(new String[0]);
    }



}
