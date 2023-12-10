package com.moyu.test.terminal.jline;

import org.jline.reader.CompletingParsedLine;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;

import java.util.Arrays;
import java.util.List;

/**
 * @author xiaomingzhang
 * @date 2023/12/10
 */
public class MyJlineParser implements Parser {

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) throws SyntaxError {

        // 在这里编写你的解析逻辑
        // 例如，将输入按照空格进行分割
        String[] tokens = line.split("\\s+");

         return new CompletingParsedLine() {
             @Override
             public String word() {
                 // 返回当前输入的单词
                 return tokens[cursor];
             }

             @Override
             public int wordCursor() {
                 // 返回当前单词的光标位置
                 return cursor - tokens[cursor].length();
             }

             @Override
             public int wordIndex() {
                 // 返回当前单词的索引
                 return cursor;
             }

             @Override
             public List<String> words() {
                 return Arrays.asList(tokens);
             }

             @Override
             public String line() {
                 // 返回完整的输入行
                 return line;
             }

             @Override
             public int cursor() {
                 // 返回当前光标位置
                 return cursor;
             }

             @Override
             public CharSequence escape(CharSequence candidate, boolean complete) {
                 // 该方法可以对字符进行转义
                 return candidate;
             }

             @Override
             public int rawWordCursor() {
                 // 返回不考虑特殊字符解释的当前单词的光标位置
                 return wordCursor();
             }

             @Override
             public int rawWordLength() {
                 // 实现获取当前单词在输入行中的长度的逻辑
                 // 这里使用 String#indexOf 和 String#lastIndexOf 获取单词在输入行中的位置
                 int start = line.lastIndexOf(' ', cursor - 1) + 1;
                 int end = line.indexOf(' ', cursor);
                 end = (end == -1) ? line.length() : end;
                 return end - start;
             }
         };
    }
}
