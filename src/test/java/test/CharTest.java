package test;

/**
 * @author xiaomingzhang
 * @date 2023/6/1
 */
public class CharTest {

    public static void main(String[] args) {
        String str = "\uD83C\uDF7A";
        System.out.println("char:" + str);
        System.out.println("char len:" + str.length());
        System.out.println("byte len:" + str.getBytes().length);;

    }


}
