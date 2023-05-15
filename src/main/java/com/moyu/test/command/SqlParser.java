package com.moyu.test.command;

import com.moyu.test.command.ddl.CreateDatabaseCommand;
import com.moyu.test.command.ddl.DropDatabaseCommand;
import com.moyu.test.command.ddl.ShowDatabasesCommand;

import java.util.Arrays;

/**
 * @author xiaomingzhang
 * @date 2023/5/15
 */
public class SqlParser implements Parser {

    private String sql;

    private char[] sqlCharArr;

    private int currIndex;

    private static final String CREATE = "CREATE";
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";
    private static final String SELECT = "SELECT";

    private static final String DROP = "DROP";
    private static final String SHOW = "SHOW";

    private static final String DATABASE = "DATABASE";
    private static final String TABLE = "TABLE";


    @Override
    public Command prepareCommand(String sql) {
        this.sql = sql.toUpperCase();
        this.sqlCharArr = this.sql.toCharArray();
        this.currIndex = 0;

        String firstKeyWord = getNextKeyWord();

        switch (firstKeyWord) {
            case CREATE:
                skipSpace();
                String nextKeyWord = getNextKeyWord();
                switch (nextKeyWord) {
                    case DATABASE:
                        skipSpace();
                        String databaseName = getNextKeyWord();
                        CreateDatabaseCommand command = new CreateDatabaseCommand();
                        command.setDatabaseName(databaseName);
                        return command;
                    case TABLE:
                        break;
                    default:
                        throw new RuntimeException("sql语法有误");
                }
                break;
            case UPDATE:
                break;
            case DELETE:
                break;
            case SELECT:
                break;
            case DROP:
                skipSpace();
                String keyWord = getNextKeyWord();
                switch (keyWord) {
                    case DATABASE:
                        skipSpace();
                        String databaseName = getNextKeyWord();
                        DropDatabaseCommand command = new DropDatabaseCommand();
                        command.setDatabaseName(databaseName);
                        return command;
                    case TABLE:
                        break;
                    default:
                        throw new RuntimeException("sql语法有误");
                }
                break;
            case SHOW:
                skipSpace();
                String word11 = getNextKeyWord();
                if ("DATABASES".equals(word11)) {
                    return new ShowDatabasesCommand();
                } else if ("TABLES".equals(word11)) {

                }
                break;
            default:
                throw new RuntimeException("sql语法有误");
        }
        return null;
    }






    private String getNextKeyWord() {
        int i = currIndex;
        while (i < sqlCharArr.length) {
            if (sqlCharArr[i] == ' ') {
                break;
            }
            if (sqlCharArr[i] == ';') {
                break;
            }
            i++;
        }
        String word = new String(sqlCharArr, currIndex, i - currIndex);
        currIndex = i;
        return word;
    }


    private void skipSpace() {
        while (currIndex < sqlCharArr.length) {
            if (sqlCharArr[currIndex] != ' ') {
                break;
            }
            currIndex++;
        }
    }


    public static void main(String[] args) {

        SqlParser sqlParser = new SqlParser();
        Command command1 = sqlParser.prepareCommand("drop database xmz");
        command1.exec();

        Command command2 = sqlParser.prepareCommand("show databases");
        String[] exec = command2.exec();
        Arrays.asList(exec).forEach(System.out::println);
    }




}
