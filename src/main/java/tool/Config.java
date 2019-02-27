package tool;
import java.sql.*;
public class Config {
    public static final  String driverName = "org.apache.hive.jdbc.HiveDriver";//hive驱动名称
    public static final String url = "jdbc:hive2://192.168.199.173:10000/default";
    public static final String user = "root";
    public static final String password = "123";
    public static Connection getHiveInstance() throws Exception{
        Class.forName("org.apache.hive.jdbc.HiveDriver") ;
        Connection con = DriverManager.getConnection(url, "hadoop", "hadoop");
        return con;
    }


    public static void main (String args[]) throws Exception{

        String sql = "select * from temp" ;
        Connection myHiveCon = getHiveInstance();
        myHiveCon.setAutoCommit(false);
        Statement stHive = myHiveCon.createStatement();
        ResultSet rs = stHive.executeQuery(sql);
        while (rs.next())
            System.out.println(rs.getString("id") + " , " + rs.getDouble("age"));
        System.out.println();
    }
}
