package test;

/**
 * @author xiaomingzhang
 * @date 2023/6/1
 */
public class CharTest {

    public static void main(String[] args) {

        String str = "aaa as  bbb";
        String[] s = str.split("\\s+");
        System.out.println(s.length);

        for (String s1 : s) {
            System.out.println(s1);
        }


/*        String str = "\uD83C\uDF7A";
        System.out.println("char:" + str);
        System.out.println("char len:" + str.length());
        System.out.println("byte len:" + str.getBytes().length);;*/

    }


}
