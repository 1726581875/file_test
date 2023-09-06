package test;

import com.moyu.test.command.dml.sql.Query;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Deque;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaomingzhang
 * @date 2023/6/1
 */
public class CharTest {

    public static void main(String[] args) {


/*        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        LocalDate date1 = LocalDate.parse("2023-05-31", formatter);
        LocalDate date2 = LocalDate.parse("2023-07-31", formatter);

        long daysDiff = ChronoUnit.MONTHS.between(date1, date2);

        System.out.println(daysDiff);*/

/*        String str = "\uD83C\uDF7A";
        System.out.println("char:" + str);
        System.out.println("char len:" + str.length());
        System.out.println("byte len:" + str.getBytes().length);;*/


        Deque<Integer> queryStack = new LinkedList<>();
        queryStack.push(1);
        queryStack.push(2);
        queryStack.push(3);

        System.out.println(queryStack.pop());
        System.out.println(queryStack.pop());
        System.out.println(queryStack.pop());


        System.out.println("'啊啊啊啊'");


    }


    private static boolean likeTest(String sqlPattern, String input){
        String pattern = sqlPattern.replace("%", ".*");
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(input);
        return matcher.matches();
    }


}
