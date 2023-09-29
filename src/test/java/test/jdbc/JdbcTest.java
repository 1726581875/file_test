package test.jdbc;

import java.sql.*;

/**
 * @author xiaomingzhang
 * @date 2023/9/28
 */
public class JdbcTest {

    private static final String driver = "com.moyu.test.jdbc.Driver";
    private static final String url = "localhost:8888:ddd";



    public static void main(String[] args) {
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, null, null);
            PreparedStatement statement = conn.prepareStatement("select * from test where id = ?;");
            statement.setObject(1, 1);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String name = resultSet.getString(2);
                System.out.println(id + "," + name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    private static void simpleTest(){
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, null, null);
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from xmz_d_t;");
            while (resultSet.next()) {
                Integer id = resultSet.getInt(1);
                String name = resultSet.getString(2);
                Date date = resultSet.getDate(3);
                System.out.println(id + "," + name + "," + date);

                Integer id2 = resultSet.getInt("id");
                String name2 = resultSet.getString("name");
                Date date2 = resultSet.getDate("time");
                System.out.println(id2 + "," + name2 + "," + date2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
