package abnormal;

import tool.Config;


import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class Abnormal {


    /**
     * 求某个表某个字段的平均值
     * @param flied
     * @param tablename
     */
    public static int countMean(String flied,String tablename){
        try {
            Class.forName(Config.driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
            String sql = "select avg( " + flied +" ) as mean from " + tablename;
            PreparedStatement pstm = conn.prepareStatement(sql);
            ResultSet rs = pstm.executeQuery();
            rs.next();
            int mean = rs.getInt("mean");
            pstm.close();
            conn.close();
            rs.close();
            return mean;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
            return -1;
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
            return -1;
        }
    }
    /**
     * 计算某多值离散属性的众数
     * @param flied
     * @param tablename
     */
    public static String countmedio(String flied,String tablename){
        try {
            Class.forName(Config.driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
            String sql = "select avg( " + flied +" ) as mean from " + tablename;
            PreparedStatement pstm = conn.prepareStatement(sql);
            ResultSet rs = pstm.executeQuery();
            rs.next();
            String mid = rs.getString("");
            pstm.close();
            conn.close();
            rs.close();
            return mid;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * 替换小于
     * @param flied
     * @param tablename
     * @param min
     * @param mean
     */
    public static void placeMin(String flied,String tablename,int min,int mean){
        try {
            Class.forName(Config.driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
            conn.setAutoCommit(false);
            String sql = "select id from "+tablename + " where "+ flied + " < " + String.valueOf(min);
            Statement pstm = conn.createStatement();
            ResultSet rs = pstm.executeQuery(sql);
            sql = "update "+ tablename + " set "+flied + " = " + mean  + " where id = ";
            while(rs.next()){
                int id = rs.getInt("id");
                String temp = sql + String.valueOf(id);
                pstm.executeUpdate(temp);
            }
            pstm.close();
            rs.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 替换大于
     * @param flied
     * @param tablename
     * @param max
     * @param mean
     */
    public static void placeMax(String flied,String tablename,int max,int mean){
        try {
            Class.forName(Config.driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
            conn.setAutoCommit(false);
            String sql = "select id from "+tablename + " where "+ flied + " > " + String.valueOf(max);
            Statement pstm = conn.createStatement();
            ResultSet rs = pstm.executeQuery(sql);
            sql = "update "+ tablename + " set "+flied + " = " + mean  + " where id = ";
            while(rs.next()){
                int id = rs.getInt("id");
                String temp = sql + String.valueOf(id);
                pstm.executeUpdate(temp);
            }
            pstm.close();
            rs.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * 长度异常
     * @param flied
     * @param tablename
     * @param n
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void isLong(String flied,String tablename,int n) throws SQLException,ClassNotFoundException{
        String sql = "select "+flied + " from "+tablename ;
        Class.forName(Config.driverName);//加载HiveServer2驱动程序
        Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
        conn.setAutoCommit(false);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        sql = "update "+tablename + " set "+flied + " = ? where " + flied +" = ?";
        PreparedStatement pstm = conn.prepareStatement(sql);
        while(rs.next()){
            String id = rs.getString(flied);
            //填充值
            int newid = 0;
            pstm.setInt(1,newid);
            pstm.setInt(2,Integer.valueOf(id));
            pstm.executeUpdate();
        }
        conn.close(); rs.close();pstm.close();
    }

    /**
     * 日期格式异常
     * 处理方式为直接删除所在行
     * @param flied
     * @param tablename
     */
    public static void isValidDateDelete(String flied,String tablename)throws SQLException,ClassNotFoundException{
        String sql = "select id,"+flied + " from "+tablename ;
        Class.forName(Config.driverName);//加载HiveServer2驱动程序
        Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
        conn.setAutoCommit(false);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        while(rs.next()){
            String id = rs.getString("id");
            String str = rs.getString(flied);
            if(qualifyDate(str)==false){
                sql = "delete from "+tablename + " where id = " + id;
                conn.createStatement().execute(sql);
            }
        }
        conn.close(); rs.close();
    }

    /**
     * 日期格式异常
     * 用众数填充
     * @param flied
     * @param tablename
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void isValidDateMean(String flied,String tablename)throws SQLException,ClassNotFoundException{
        String sql = "select id,"+flied + " from "+tablename ;
        Class.forName(Config.driverName);//加载HiveServer2驱动程序
        Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
        conn.setAutoCommit(false);
        ResultSet rs = conn.createStatement().executeQuery(sql);
        sql = "update "+tablename + " set "+flied + " = ? where id = ?";
        PreparedStatement pstm = conn.prepareStatement(sql);
        while(rs.next()){
            String id = rs.getString("id");
            String str = rs.getString(flied);
            if(qualifyDate(str)== false) {
                //填充值
                int newdate = 0;
                pstm.setInt(1, newdate);
                pstm.setInt(2, Integer.valueOf(id));
                pstm.executeUpdate();
            }
        }
        conn.close(); rs.close();pstm.close();
    }
    public static boolean qualifyDate(String str) {
        boolean convertSuccess=true;
        // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
        SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        try {
            // 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
            format.setLenient(false);
            format.parse(str);
        } catch (ParseException e) {
            // e.printStackTrace();
// 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            convertSuccess=false;
        }
        return convertSuccess;
    }

    public static void main(String[] args) {

        //placeMin("age","students",25,23);
        countMean("age","students");
    }
}