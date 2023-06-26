package test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiaomingzhang
 * @date 2023/6/1
 */
public class CharTest {

    public static void main(String[] args) {

/*        String str = "aaa as  bbb";
        String[] s = str.split("\\s+");
        System.out.println(s.length);

        for (String s1 : s) {
            System.out.println(s1);
        }*/


/*        String str = "\uD83C\uDF7A";
        System.out.println("char:" + str);
        System.out.println("char len:" + str.length());
        System.out.println("byte len:" + str.getBytes().length);;*/


        System.out.println(likeTest("hello","HELLO1"));
        System.out.println(likeTest("he%", "hello"));

    }


    private static boolean likeTest(String sqlPattern, String input){
        String pattern = sqlPattern.replace("%", ".*");
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(input);
        return matcher.matches();
    }


}
