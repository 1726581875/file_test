package com.moyu.xmz.common.exception;

/**
 * @author xiaomingzhang
 * @date 2023/8/18
 */
public class ExceptionUtil {


    public static DbException buildDbException(String msgTemple, Object... params) {
        return new DbException(buildExceptionMsg(msgTemple, params));
    }

    public static void throwDbException(String msgTemple, Object... params) {
        throw new DbException(buildExceptionMsg(msgTemple, params));
    }

    public static void throwSqlIllegalException(String msgTemple, Object... params) {
        throw new SqlIllegalException(buildExceptionMsg(msgTemple, params));
    }

    public static void throwSqlExecutionException(String msgTemple, Object... params) {
        throw new SqlExecutionException(buildExceptionMsg(msgTemple, params));
    }

    public static void throwSqlQueryException(String msgTemple, Object... params) {
        throw new SqlQueryException(buildExceptionMsg(msgTemple, params));
    }



    private static String buildExceptionMsg(String msgTemple, Object... params){
        StringBuilder msgBuilder = new StringBuilder("");
        if (msgTemple != null) {
            char[] charArray = msgTemple.toCharArray();
            int paramIndex = 0;
            for (int i = 0; i < charArray.length; i++) {
                // 遇到{}占位符
                if (charArray[i] == '{' && i != charArray.length - 1 && charArray[i + 1] == '}') {
                    if (params != null && params.length > paramIndex) {
                        Object param = params[paramIndex];
                        msgBuilder.append(String.valueOf(param));
                        paramIndex++;
                    } else {
                        msgBuilder.append("{}");
                    }
                    i += 2;
                }
                if(i < charArray.length) {
                    msgBuilder.append(charArray[i]);
                }
            }
        }
        return msgBuilder.toString();
    }





}
