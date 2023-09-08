package com.moyu.test.terminal;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/9/8
 */
public class TabCompleter implements Completer {

    private static final List<String> firstKeyWords = new LinkedList<>();

    static {
        List<String> dbKeyWordList = Arrays.asList("CREATE", "SELECT", "INSERT", "UPDATE", "DELETE", "SHOW");
        for (String keyword : dbKeyWordList) {
            firstKeyWords.add(keyword);
            firstKeyWords.add(keyword.toLowerCase());
        }
    }

    @Override
    public void complete(LineReader lineReader, ParsedLine parsedLine, List<Candidate> candidates) {
        // 获取当前输入行的文本和光标位置
        String buffer = parsedLine.line();
        int cursor = parsedLine.cursor();


        String cursorBeforeValue = buffer.substring(0, cursor);

        String[] inputWords = cursorBeforeValue.split("\\s+");
        List<String> commandCandidates = new ArrayList<>();

        // 补全第一个关键字
        if (inputWords.length <= 1) {
            commandCandidates.addAll(firstKeyWords);
        } else {
            String lastWork = null;
            if(cursorBeforeValue.endsWith(" ")) {
                lastWork = inputWords[inputWords.length - 1];
            } else {
                // 取上一个词，预测下一个词的可能值
                lastWork = inputWords[inputWords.length - 2];
            }
            // 根据前面输入的值提示补全输入
            switch (lastWork.toUpperCase()) {
                case "CREATE":
                    commandCandidates.addAll(getKeyWordsAfterCreate());
                    break;
                case "SHOW":
                    commandCandidates.addAll(getKeyWordsAfterShow());
                    break;
            }
        }

        // 将候选项添加到补全列表
        for (String commandCandidate : commandCandidates) {
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
