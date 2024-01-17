package com.moyu.xmz.terminal.jline;

import org.jline.reader.*;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.reader.impl.ReaderUtils;
import org.jline.terminal.Terminal;
import org.jline.utils.Log;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xiaomingzhang
 * @date 2023/11/21
 * jLine 使用tab键补全内容时候后面会输出多一个空格。
 * 为了实现去掉空格，继承并重写doComplete方法注释掉一行代码 buf.write(" ");
 *
 */
public class MyLineReaderImpl extends LineReaderImpl {

    public MyLineReaderImpl(Terminal terminal) throws IOException {
        super(terminal);
    }

    public MyLineReaderImpl(Terminal terminal, String appName) throws IOException {
        super(terminal, appName);
    }

    public MyLineReaderImpl(Terminal terminal, String appName, Map<String, Object> variables) {
        super(terminal, appName, variables);
    }

    @Override
    protected boolean doComplete(CompletionType lst, boolean useMenu, boolean prefix, boolean forSuggestion) {
        // If completion is disabled, just bail out
        if (getBoolean(DISABLE_COMPLETION, false)) {
            return true;
        }
        // Try to expand history first
        // If there is actually an expansion, bail out now
        if (!isSet(Option.DISABLE_EVENT_EXPANSION)) {
            try {
                if (expandHistory()) {
                    return true;
                }
            } catch (Exception e) {
                Log.info("Error while expanding history", e);
                return false;
            }
        }

        // Parse the command line
        CompletingParsedLine line;
        try {
            line = wrap(parser.parse(buf.toString(), buf.cursor(), Parser.ParseContext.COMPLETE));
        } catch (Exception e) {
            Log.info("Error while parsing line", e);
            return false;
        }

        // Find completion candidates
        List<Candidate> candidates = new ArrayList<>();
        try {
            if (completer != null) {
                completer.complete(this, line, candidates);
            }
        } catch (Exception e) {
            Log.info("Error while finding completion candidates", e);
            if (Log.isDebugEnabled()) {
                e.printStackTrace();
            }
            return false;
        }

        if (lst == CompletionType.ExpandComplete || lst == CompletionType.Expand) {
            String w = expander.expandVar(line.word());
            if (!line.word().equals(w)) {
                if (prefix) {
                    buf.backspace(line.wordCursor());
                } else {
                    buf.move(line.word().length() - line.wordCursor());
                    buf.backspace(line.word().length());
                }
                buf.write(w);
                return true;
            }
            if (lst == CompletionType.Expand) {
                return false;
            } else {
                lst = CompletionType.Complete;
            }
        }

        boolean caseInsensitive = isSet(Option.CASE_INSENSITIVE);
        int errors = getInt(ERRORS, DEFAULT_ERRORS);

        completionMatcher.compile(options, prefix, line, caseInsensitive, errors, getOriginalGroupName());
        // Find matching candidates
        List<Candidate> possible = completionMatcher.matches(candidates);
        // If we have no matches, bail out
        if (possible.isEmpty()) {
            return false;
        }
        size.copy(terminal.getSize());
        try {
            // If we only need to display the list, do it now
            if (lst == CompletionType.List) {
                doList(possible, line.word(), false, line::escape, forSuggestion);
                return !possible.isEmpty();
            }

            // Check if there's a single possible match
            Candidate completion = null;
            // If there's a single possible completion
            if (possible.size() == 1) {
                completion = possible.get(0);
            }
            // Or if RECOGNIZE_EXACT is set, try to find an exact match
            else if (isSet(Option.RECOGNIZE_EXACT)) {
                completion = completionMatcher.exactMatch();
            }
            // Complete and exit
            if (completion != null && !completion.value().isEmpty()) {
                if (prefix) {
                    buf.backspace(line.rawWordCursor());
                } else {
                    buf.move(line.rawWordLength() - line.rawWordCursor());
                    buf.backspace(line.rawWordLength());
                }
                buf.write(line.escape(completion.value(), completion.complete()));
                if (completion.complete()) {
                    if (buf.currChar() != ' ') {
                        // 匹配到字符后不需要后面加空格
                        //buf.write(" ");
                    } else {
                        buf.move(1);
                    }
                }
                if (completion.suffix() != null) {
                    if (autosuggestion == SuggestionType.COMPLETER) {
                        listChoices(true);
                    }
                    redisplay();
                    Binding op = readBinding(getKeys());
                    if (op != null) {
                        String chars = getString(REMOVE_SUFFIX_CHARS, DEFAULT_REMOVE_SUFFIX_CHARS);
                        String ref = op instanceof Reference ? ((Reference) op).name() : null;
                        if (SELF_INSERT.equals(ref) && chars.indexOf(getLastBinding().charAt(0)) >= 0
                                || ACCEPT_LINE.equals(ref)) {
                            buf.backspace(completion.suffix().length());
                            if (getLastBinding().charAt(0) != ' ') {
                                buf.write(' ');
                            }
                        }
                        pushBackBinding(true);
                    }
                }
                return true;
            }

            if (useMenu) {
                buf.move(line.word().length() - line.wordCursor());
                buf.backspace(line.word().length());
                doMenu(possible, line.word(), line::escape);
                return true;
            }

            // Find current word and move to end
            String current;
            if (prefix) {
                current = line.word().substring(0, line.wordCursor());
            } else {
                current = line.word();
                buf.move(line.rawWordLength() - line.rawWordCursor());
            }
            // Now, we need to find the unambiguous completion
            // TODO: need to find common suffix
            String commonPrefix = completionMatcher.getCommonPrefix();
            boolean hasUnambiguous = commonPrefix.startsWith(current) && !commonPrefix.equals(current);

            if (hasUnambiguous) {
                buf.backspace(line.rawWordLength());
                buf.write(line.escape(commonPrefix, false));
                callWidget(REDISPLAY);
                current = commonPrefix;
                if ((!isSet(Option.AUTO_LIST) && isSet(Option.AUTO_MENU))
                        || (isSet(Option.AUTO_LIST) && isSet(Option.LIST_AMBIGUOUS))) {
                    if (!nextBindingIsComplete()) {
                        return true;
                    }
                }
            }
            if (isSet(Option.AUTO_LIST)) {
                if (!doList(possible, current, true, line::escape)) {
                    return true;
                }
            }
            if (isSet(Option.AUTO_MENU)) {
                buf.backspace(current.length());
                doMenu(possible, line.word(), line::escape);
            }
            return true;
        } finally {
            size.copy(terminal.getBufferSize());
        }
    }


    @Override
    protected String finish(String str) {
        String historyLine = str;

/*        if (!isSet(Option.DISABLE_EVENT_EXPANSION)) {
            StringBuilder sb = new StringBuilder();
            boolean escaped = false;
            for (int i = 0; i < str.length(); i++) {
                char ch = str.charAt(i);
                if (escaped) {
                    escaped = false;
                    if (ch != '\n') {
                        sb.append(ch);
                    }
                } else if (parser.isEscapeChar(ch)) {
                    escaped = true;
                } else {
                    sb.append(ch);
                }
            }
            str = sb.toString();
        }*/

        if (maskingCallback != null) {
            historyLine = maskingCallback.history(historyLine);
        }

        // we only add it to the history if the buffer is not empty
        if (historyLine != null && historyLine.length() > 0 ) {
            history.add(Instant.now(), historyLine);
        }
        return str;
    }


    boolean getBoolean(String name, boolean def) {
        return ReaderUtils.getBoolean(this, name, def);
    }

    int getInt(String name, int def) {
        return ReaderUtils.getInt(this, name, def);
    }

    private boolean listChoices(boolean forSuggestion) {
        return doComplete(CompletionType.List, isSet(Option.MENU_COMPLETE), false, forSuggestion);
    }

    String getString(String name, String def) {
        return ReaderUtils.getString(this, name, def);
    }

    private void pushBackBinding(boolean skip) {
        String s = getLastBinding();
        if (s != null) {
            bindingReader.runMacro(s);
            skipRedisplay = skip;
        }
    }

}
