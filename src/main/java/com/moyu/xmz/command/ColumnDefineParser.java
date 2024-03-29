package com.moyu.xmz.command;

import com.moyu.xmz.common.constant.ColumnTypeEnum;
import com.moyu.xmz.common.constant.DbTypeConstant;
import com.moyu.xmz.common.exception.ExceptionUtil;
import com.moyu.xmz.store.common.dto.Column;

/**
 * @author xiaomingzhang
 * @date 2024/3/7
 */
public class ColumnDefineParser {

    private String columnDefineStr;

    private int columnIndex;

    private char[] columnChars;

    private int curIdx = 0;

    private static final String NOT = "NOT";
    private static final String NULL = "NULL";
    private static final String DEFAULT = "DEFAULT";
    private static final String UNSIGNED = "UNSIGNED";
    private static final String PRIMARY = "PRIMARY";
    private static final String KEY = "KEY";
    private static final String COMMENT = "COMMENT";
    private static final String AUTO_INCREMENT = "AUTO_INCREMENT";


    public ColumnDefineParser(String columnDefineStr, int columnIndex) {
        this.columnDefineStr = columnDefineStr;
        this.columnIndex = columnIndex;
        this.columnChars = columnDefineStr.trim().toCharArray();
        this.curIdx = 0;
    }

    public Column parseColumnDefine() {
        // 1、解析字段名
        String columnName = null;
        if (columnChars[0] == '`' || columnChars[0] == '"') {
            int nextCharIdx = getNextCharIdx(columnChars[0]);
            if (nextCharIdx == -1) {
                ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
            }
            columnName = columnDefineStr.substring(curIdx + 1, nextCharIdx);
            curIdx = nextCharIdx + 1;
        } else {
            columnName = getNextWord(true);
            if (columnName == null || columnName.length() == 0) {
                ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
            }
        }
        // 跳过中间空格
        skipSpace();
        // 2、解析字段类型、字段长度。示例:varchar(64)、int
        int columnLength = -1;
        // 括号开始
        int start = -1;
        // 括号结束
        int end = -1;
        for (int i = curIdx; i < columnChars.length; i++) {
            if (columnChars[i] == '(') {
                start = i;
            }
            if (columnChars[i] == ')') {
                end = i;
            }

        }
        String typeName = null;

        if (end > start && start > 0) {
            columnLength = Integer.valueOf(columnDefineStr.substring(start + 1, end));
            typeName = columnDefineStr.substring(curIdx, start);
            curIdx = end + 1;
        } else {
            typeName = getNextWord(true);
            if (typeName == null || typeName.length() == 0) {
                ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
            }
        }

        Byte type = ColumnTypeEnum.getColumnTypeByName(typeName.toLowerCase());
        if (type == null) {
            ExceptionUtil.throwSqlIllegalException("不支持类型:{},在{}附近", typeName, columnDefineStr);
        }
        // 如果没有指定字段长度像int、bigint等，要自动给长度
        if (columnLength == -1) {
            if (DbTypeConstant.INT_4 == type) {
                columnLength = 4;
            }
            if (DbTypeConstant.INT_8 == type) {
                columnLength = 8;
            }
        }
        Column column = new Column(columnName, type, this.columnIndex, columnLength);
        // 解析下一个词
        String nextWord = getNextWord(false);
        switch (nextWord.toUpperCase()) {
            case UNSIGNED:
                paresUnsigned(column);
                break;
            case PRIMARY:
                paresPrimary(column);
                break;
            case NOT:
                paresNotNull(column);
                break;
            case DEFAULT:
                paresDefault(column);
                break;
            case COMMENT:
                paresComment(column);
                break;
            case "":
                break;
            default:
                ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
                break;
        }
        return column;
    }


    private void paresUnsigned(Column column) {
        String word = getNextWord(true);
        if (!UNSIGNED.equals(word.toUpperCase())) {
            ExceptionUtil.throwDbException("解析字段异常, 下个关键字应当为{}", UNSIGNED);
        }
        if (column.getColumnType() == DbTypeConstant.INT_4) {
            column.setColumnType(DbTypeConstant.UNSIGNED_INT_4);
        } else if (column.getColumnType() == DbTypeConstant.INT_8) {
            column.setColumnType(DbTypeConstant.UNSIGNED_INT_8);
        }
        // 解析下一个词
        String nextWord = getNextWord(false);
        switch (nextWord.toUpperCase()) {
            case PRIMARY:
                paresPrimary(column);
                break;
            case NOT:
                paresNotNull(column);
                break;
            case DEFAULT:
                paresDefault(column);
                break;
            case COMMENT:
                paresComment(column);
                break;
            case "":
                break;
            default:
                ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
                break;
        }
    }

    private void paresPrimary(Column column) {
        String primaryWord = getNextWord(true);
        if (!PRIMARY.equals(primaryWord.toUpperCase())) {
            ExceptionUtil.throwDbException("解析字段异常, 下个关键字应当为{}", PRIMARY);
        }
        String keyWord = getNextWord(true);
        if (!KEY.equals(keyWord.toUpperCase())) {
            ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
        }
        column.setIsPrimaryKey((byte) 1);

        // 解析下一个词
        String nextWord = getNextWord(false);
        switch (nextWord.toUpperCase()) {
            case NOT:
                paresNotNull(column);
                break;
            case COMMENT:
                paresComment(column);
                break;
            case "":
                break;
            default:
                ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
                break;
        }
    }

    private void paresNotNull(Column column) {
        String word = getNextWord(true);
        if (!NOT.equals(word.toUpperCase())) {
            ExceptionUtil.throwDbException("解析字段异常, 下个关键字应当为{}", NOT);
        }
        String keyWord = getNextWord(true);
        if (!NULL.equals(keyWord.toUpperCase())) {
            ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
        }
        column.setIsNotNull((byte) 1);

        // 解析下一个词
        String nextWord = getNextWord(false);
        switch (nextWord.toUpperCase()) {
            case PRIMARY:
                paresPrimary(column);
                break;
            case DEFAULT:
                paresDefault(column);
                break;
            case COMMENT:
                paresComment(column);
                break;
            case AUTO_INCREMENT:
                //ExceptionUtil.throwSqlIllegalException("暂不支持AUTO_INCREMENT关键字,在{}附近", columnDefineStr);
                break;
            case "":
                break;
            default:
                //ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
                break;
        }
    }


    private void paresDefault(Column column) {

        String curWord = getNextWord(true);
        if (!DEFAULT.equals(curWord.toUpperCase())) {
            ExceptionUtil.throwDbException("解析字段异常, 下个关键字应当为{}", DEFAULT);
        }
        skipSpace();
        int i = curIdx;
        boolean isOpen = false;
        String defaultValue = null;
        while (i < columnChars.length) {
            if (columnChars[i] == '\'' && !isOpen) {
                isOpen = true;
            } else if (columnChars[i] == '\'' && isOpen) {
                isOpen = false;
                defaultValue = columnDefineStr.substring(curIdx, i + 1);
                curIdx = i + 1;
                break;
            }
            if (columnChars[i] == ' ' && !isOpen) {
                defaultValue = columnDefineStr.substring(curIdx, i + 1);
                curIdx = i;
                break;
            }

            if (i == columnChars.length - 1) {
                defaultValue = columnDefineStr.substring(curIdx, columnChars.length);
                curIdx = i + 1;
                break;
            }
            i++;
        }
        if (defaultValue == null) {
            ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
        }
        if (NULL.equals(defaultValue.toUpperCase())) {
            if (column.getIsNotNull() == (byte) 1) {
                ExceptionUtil.throwSqlIllegalException("字段{}不能允许为空，检查是否使NOT NULL和DEFAULT NULL同时使用", column.getColumnName());
            }
            column.setIsNotNull((byte) 0);
            column.setDefaultVal(NULL);
        } else {
            column.setDefaultVal(defaultValue);
        }
        column.setIsNotNull((byte) 1);

        // 解析下一个词
        String nextWord = getNextWord(false);
        switch (nextWord.toUpperCase()) {
            case COMMENT:
                paresComment(column);
                break;
            case "":
                break;
            default:
                //ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
                break;
        }

    }

    private void paresComment(Column column) {
        // 解析comment字段
        String curWord = getNextWord(true);
        if (!COMMENT.equals(curWord.toUpperCase())) {
            ExceptionUtil.throwDbException("解析字段异常, 下个关键字应当为{}", COMMENT);
        }
        skipSpace();
        int i = curIdx;
        int commentStartIdx = -1;
        Character character = null;
        boolean isOpen = false;
        String commentValue = null;
        while (i < columnChars.length) {
            if ((columnChars[i] == '\'' || columnChars[i] == '"') && !isOpen) {
                commentStartIdx = i + 1;
                isOpen = true;
                character = columnChars[i];
            } else if (isOpen && character != null && columnChars[i] == character) {
                isOpen = false;
                commentValue = columnDefineStr.substring(commentStartIdx, i);
                curIdx = i + 1;
                break;
            }
            i++;
        }
        if (commentValue == null) {
            ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
        }


        String nextWord = getNextWord(false);
        if (nextWord != null && nextWord.length() > 0) {
            ExceptionUtil.throwSqlIllegalException("sql语法异常,在{}附近", columnDefineStr);
        }
    }


    private void skipSpace() {
        while (curIdx < columnChars.length) {
            if (columnChars[curIdx] != ' ') {
                break;
            }
            curIdx++;
        }
    }

    private String getNextWord(boolean isMovePointer) {
        skipSpace();
        if (this.curIdx >= columnChars.length) {
            return "";
        }
        int i = this.curIdx;
        while (i < columnChars.length) {
            if (columnChars[i] == ' ') {
                String word = columnDefineStr.substring(this.curIdx, i);
                if (isMovePointer) {
                    this.curIdx = i;
                }
                return word;
            }
            i++;
        }
        String word = columnDefineStr.substring(curIdx, columnChars.length);
        if (isMovePointer) {
            this.curIdx = columnChars.length;
        }
        return word;
    }


    private int getNextCharIdx(char c) {
        int idx = -1;
        for (int i = curIdx + 1; i < columnChars.length; i++) {
            if (columnChars[i] == c) {
                return i;
            }
        }
        return idx;
    }


    @Override
    public String toString() {
        return columnDefineStr.substring(0, curIdx) + "[*]" + columnDefineStr.substring(curIdx, columnDefineStr.length());
    }


}
