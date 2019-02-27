package demo;

import tool.Config;

import java.io.*;
import java.sql.*;

public class CreateTable {
    public static void createTable(){
        try {
            Class.forName(Config.driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
            String sql = "create table student (id int,age int,sex int,birth date,tel string,addr string)";
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.execute();
            pstm.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    public static void insertData(){
        try {
            Class.forName(Config.driverName);//加载HiveServer2驱动程序
            Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
            String sql = "";
            PreparedStatement pstm = conn.prepareStatement(sql);
            pstm.execute();
            pstm.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader((new FileReader("C:\\Users\\summer\\Desktop\\1.txt")));
            String line = br.readLine();
            BufferedWriter out = new BufferedWriter(new FileWriter("C:\\Users\\summer\\Desktop\\2.txt"));
            String[] items = line.split(",");
            String nstr = items[0]+"/t"+items[1] + "/t" + items[2] + "/t";

        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
