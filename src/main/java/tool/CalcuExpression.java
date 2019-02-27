package tool;

import java.util.HashMap;
import java.util.Stack;

public class CalcuExpression {

    public static String countTimeExpression(String[] expression,HashMap<String,String> map){
        String result = new String();
        Stack<String> num = new Stack<String>();
        for(int i = 0; i < expression.length ; i++){
            //数字情况
            if((expression[i].equals("+")==false)|
                    (expression[i].equals("-")==false)|
                    (expression[i].equals("*")==false)|
                    (expression[i].equals("/")==false)){
                num.push(map.get(expression[i]));
            }
            else{
                if(expression[i].equals("+")){
                    long temp = SimpleDateUtil.convert2long(num.pop(),SimpleDateUtil.DATE_FORMAT)
                            + SimpleDateUtil.convert2long(num.pop(),SimpleDateUtil.DATE_FORMAT);
                    result = SimpleDateUtil.convert2String(temp,SimpleDateUtil.DATE_FORMAT);
                }
                else if(expression[i].equals("-")){
                    long temp = SimpleDateUtil.convert2long(num.pop(),SimpleDateUtil.DATE_FORMAT)
                            - SimpleDateUtil.convert2long(num.pop(),SimpleDateUtil.DATE_FORMAT);
                    result = SimpleDateUtil.convert2String(temp,SimpleDateUtil.DATE_FORMAT);
                }
                num.push(result);
            }
        }

        return result;
    }
    public static int countExpression(String[] expression, HashMap<String,Integer> map){
        int result = 0;
        Stack<Integer> num = new Stack<Integer>();
        for(int i = 0; i < expression.length ; i++){
            //数字情况
            if((expression[i].equals("+")==false)|
                    (expression[i].equals("-")==false)|
                    (expression[i].equals("*")==false)|
                    (expression[i].equals("/")==false)){
                num.push(map.get(expression[i]));
            }
            else{
                result = cal(num.pop(),num.pop(),expression[i]);
                num.push(result);
            }
        }
        return result;
    }
    /**
     * 判断运算符优先级
     * @param c
     * @return
     */
    public static int isType(String c){
        if(c.equals("+") || c.equals("-")){
            return 1;
        }else if (c.equals("*") || c.equals("/")){
            return 2;
        }else {
            return 0;
        }
    }
    /**
     * 运算
     * @param n
     * @param m
     * @param c
     * @return
     */
    public static int cal(int n,int m, String c){
        int result = 0;
        if(c.equals("+")){
            result = n + m;
        }
        else if(c.equals("-")){
            result = n - m;
        }
        else if(c.equals("*")){
            result = n * m;
        }
        else if(c.equals("/")){
            result = n / m;
        }
        return result;
    }
}
