package test.jdbc;

import java.sql.*;

/**
 * @author xiaomingzhang
 * @date 2023/9/28
 */
public class JdbcTest {

    private static final String driver = "com.moyu.test.jdbc.Driver";
    private static final String url = "localhost:8888:aaa";



    public static void main(String[] args) {
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
