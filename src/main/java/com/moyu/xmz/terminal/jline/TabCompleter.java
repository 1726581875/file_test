package com.moyu.xmz.terminal.jline;

import com.moyu.xmz.net.model.terminal.DatabaseInfo;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.*;

/**
 * @author xiaomingzhang
 * @date 2023/9/8
 * 定义JLine按下终端输入时候按下Tab键之后补全规则
 */
public class TabCompleter implements Completer {

    private DatabaseInfo database;

    private static final List<String> firstKeyWords = new LinkedList<>();

    static {
        List<String> dbKeyWordList = Arrays.asList("CREATE", "SELECT", "INSERT", "UPDATE", "DELETE", "SHOW");
        for (String keyword : dbKeyWordList) {
            firstKeyWords.add(keyword);
            firstKeyWords.add(keyword.toLowerCase());
        }
    }

    public TabCompleter(DatabaseInfo database) {
        this.database = database;
    }

    @Override
    public void complete(LineReader lineReader, ParsedLine parsedLine, List<Candidate> candidates) {
        // 获取当前输入行的文本和光标位置
        String buffer = parsedLine.line();
        int cursor = parsedLine.cursor();

        // 按下tab键当前可以匹配命中的值
        List<String> optionalValues = getNextOptionalWords(buffer, cursor);

        // 将候选项添加到补全列表
        for (String commandCandidate : optionalValues) {
            candidates.add(new Candidate(
                    // 候选项的显示文本
                    commandCandidate,
                    // 候选项的修饰文本
                    commandCandidate,
                    // 候选项的描述文本
                    null,
                    // 候选项的图标（可选）
                    null,
                    // 候选项的后缀（可选）
                    null,
                    // 候选项的类型（可选）
                    null,
                    // 候选项是否需要进行引号转义（可选）
                    true
            ));
        }
    }


    private List<String> getNextOptionalWords(String buffer, int cursor) {

        List<String> optionalValues = new ArrayList<>();

        String cursorBeforeValue = buffer.substring(0, cursor);

        String[] inputWords = cursorBeforeValue.split("\\s+");

        // 还没选择数据库
        if (this.database == null) {
            if (inputWords.length <= 1 && !cursorBeforeValue.endsWith(" ")) {
                optionalValues.add("SHOW");
                optionalValues.add("show");
            } else {
                // 拿到最后一个完整的关键词
                String lastWord = null;
                if (isEndWithSpace(cursorBeforeValue)) {
                    lastWord = inputWords[inputWords.length - 1];
                } else {
                    lastWord = inputWords[inputWords.length - 2];
                }
                if (lastWord.toUpperCase().equals("SHOW")) {
                    optionalValues.add("DATABASES");
                    optionalValues.add("databases");
                }
            }
        } else /* 选择了数据库 */ {

            if (inputWords.length <= 1 && !isEndWithSpace(cursorBeforeValue)) {
                // 当正在输入第一个词，返回第一个词可匹配项
                optionalValues.addAll(firstKeyWords);
            } else {
                // 拿到最后一个完整的关键词
                String lastWord = null;
                if (isEndWithSpace(cursorBeforeValue)) {
                    lastWord = inputWords[inputWords.length - 1];
                } else {
                    lastWord = inputWords[inputWords.length - 2];
                }
                // 根据完整的关键词，预测下一个词补全选项
                switch (lastWord.toUpperCase()) {
                    case "SELECT":
                        optionalValues.addAll(getKeyWordsAfterSelect());
                        break;
                    case "CREATE":
                        optionalValues.addAll(getKeyWordsAfterCreate());
                        break;
                    case "SHOW":
                        optionalValues.addAll(getKeyWordsAfterShow());
                        break;
                    case "FROM":
                    case "DESC":
                    case "ON":
                    case "TABLE":
                        optionalValues.addAll(getKeyWordsAfterForm());
                        break;
                }
            }
        }

        return optionalValues;
    }


    private boolean isEndWithSpace(String str) {
        return str.endsWith(" ");
    }

    private List<String> getKeyWordsAfterForm() {
        List<String> tableNameList = new ArrayList<>();
        if (this.database != null) {
           return this.database.getTableNameList();
        }
        tableNameList.stream().sorted();
        return tableNameList;
    }

    private List<String> getKeyWordsAfterSelect() {
        // 常用函数
        return buildKeyWorkList(Arrays.asList("UNIX_TIMESTAMP", "FROM_UNIXTIME", "UUID", "NOW"));
    }


    private List<String> getKeyWordsAfterCreate() {
        return buildKeyWorkList(Arrays.asList("INDEX", "TABLE", "DATABASE"));
    }

    private List<String> getKeyWordsAfterShow() {
        return buildKeyWorkList(Arrays.asList("DATABASES", "TABLES"));
    }


    private List<String> buildKeyWorkList(List<String> upCaseKeyWords) {
        List<String> keyWords = new ArrayList<>();
        for (String keyword : upCaseKeyWords) {
            keyWords.add(keyword);
            keyWords.add(keyword.toLowerCase());
        }
        return keyWords;
    }

}
