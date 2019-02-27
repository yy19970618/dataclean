package consistency;

import tool.Config;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import tool.CalcuExpression;

public class Consisitent {
    /**
     * 加减乘除   等号、数值型情况
     * @param tablename
     * @param expression
     */
    public static void isNumbericalEqual(String tablename,String[] expression , String resul )throws SQLException,ClassNotFoundException{
        Class.forName(Config.driverName);//加载HiveServer2驱动程序
        Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
        //解析字段
        HashMap<String,Integer> items = new HashMap<String, Integer>();
        for(int i = 0;i< expression.length;i++){
            if((expression[i].equals("+")==false)|
                    (expression[i].equals("-")==false)|
                    (expression[i].equals("*")==false)|
                    (expression[i].equals("/")==false)){
                boolean temp = true;
                for(String e :items.keySet()){
                    if(e.equals(expression[i])){
                        temp=false;
                    }
                }
                if(temp==true)
                    items.keySet().add(expression[i]);
            }
        }
        //查询出所有相关字段
        String sql = "select ";
        for(int i = 0;i < items.size()-1;i++){
            sql = sql + items.get(i) + ",";
        }
        sql = sql + items.get(items.size()-1) + " from " + tablename;
        ResultSet rs = conn.createStatement().executeQuery(sql);
        sql = "select id,"+resul+" from "+tablename;
        ResultSet resulrs = conn.createStatement().executeQuery(sql);
        while (rs.next()&&resulrs.next()){
            for(String e : items.keySet()){
                int temp = rs.getInt(e);
                items.put(e,temp);
            }
            //求解后缀表达式
            int result = CalcuExpression.countExpression(expression,items);
            String id = resulrs.getString("id");
            //判断是否满足一致性并更新表
            int trueresul = resulrs.getInt(resul);
            if(trueresul!=result) {
                sql = "update " + tablename + " set " + resul + " = " + Integer.valueOf(result) + "where id = " + id;
                conn.createStatement().execute(sql);
            }
            items.clear();
        }
        conn.close();rs.close();
    }

    /**
     * 日期型 等号 加减
     * @param tablename
     * @param expression
     * @param resul
     */
    public static void isTimeEqual(String tablename,String[] expression,String resul)throws SQLException,ClassNotFoundException{
        //所有数据类型均为日期型数据
        Class.forName(Config.driverName);//加载HiveServer2驱动程序
        Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
        //解析字段
        HashMap<String,String> items = new HashMap<String, String>();
        for(int i = 0;i< expression.length;i++){
            if((expression[i].equals("+")==false)|
                    (expression[i].equals("-")==false)){
                boolean temp = true;
                for(String e :items.keySet()){
                    if(e.equals(expression[i])){
                        temp=false;
                    }
                }
                if(temp==true)
                    items.keySet().add(expression[i]);
            }
        }
        //查询出所有相关字段
        String sql = "select ";
        for(int i = 0;i < items.size()-1;i++){
            sql = sql + items.get(i) + ",";
        }
        sql = sql + items.get(items.size()-1) + " from " + tablename;
        ResultSet rs = conn.createStatement().executeQuery(sql);
        sql = "select id,"+resul+" from "+tablename;
        ResultSet resulrs = conn.createStatement().executeQuery(sql);
        while (rs.next()&&resulrs.next()){
            for(String e : items.keySet()){
                String temp = rs.getString(e);
                items.put(e,temp);
            }
            //求解后缀表达式
            String result = CalcuExpression.countTimeExpression(expression,items);
            String id = resulrs.getString("id");
            //判断是否满足一致性并更新表
            String trueresul = resulrs.getString(resul);
            if(trueresul.equals(result)==false) {
                sql = "update " + tablename + " set " + resul + " = " + Integer.valueOf(result) + "where id = " + id;
                conn.createStatement().execute(sql);
            }
            items.clear();
        }
        conn.close();rs.close();resulrs.close();
    }

    /**
     * 加减乘除 数值型 依赖关系下的一致性
     * @param tablename
     * @param expression
     * @param resul
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public static void isNumbericalUnequal(String tablename,String[] expression , String resul )throws SQLException,ClassNotFoundException{
        Class.forName(Config.driverName);//加载HiveServer2驱动程序
        Connection conn = DriverManager.getConnection(Config.url, Config.user, Config.password);//根据URL连接指定的数据库
        //解析字段
        HashMap<String,Integer> items = new HashMap<String, Integer>();
        for(int i = 0;i< expression.length;i++){
            if((expression[i].equals("+")==false)|
                    (expression[i].equals("-")==false)|
                    (expression[i].equals("*")==false)|
                    (expression[i].equals("/")==false)){
                boolean temp = true;
                for(String e :items.keySet()){
                    if(e.equals(expression[i])){
                        temp=false;
                    }
                }
                if(temp==true)
                    items.keySet().add(expression[i]);
            }
        }
        //查询出所有相关字段
        String sql = "select ";
        for(int i = 0;i < items.size()-1;i++){
            sql = sql + items.get(i) + ",";
        }
        sql = sql + items.get(items.size()-1) + " from " + tablename;
        ResultSet rs = conn.createStatement().executeQuery(sql);
        sql = "select id,"+resul+" from "+tablename;
        ResultSet resulrs = conn.createStatement().executeQuery(sql);
        while (rs.next()&&resulrs.next()){
            for(String e : items.keySet()){
                int temp = rs.getInt(e);
                items.put(e,temp);
            }
            //求解后缀表达式
            int result = CalcuExpression.countExpression(expression,items);
            String id = resulrs.getString("id");
            //建立一个新表存储等号两边的值
            int trueresul = resulrs.getInt(resul);
            sql = "insert into temp values ("+String.valueOf(result)+","+String.valueOf(trueresul)+")";
            try {
                conn.createStatement().execute(sql);
            } catch (SQLException e) {
                sql = "delete from temp where id = "+id;
                conn.createStatement().execute(sql);
            }
            items.clear();
        }
        sql = "delete from temp";
        conn.createStatement().execute(sql);
        conn.close();rs.close();resulrs.close();
    }

    public static void main(String[] args) {
        String[] items = {"a","+"};
        System.out.println();
    }
}
