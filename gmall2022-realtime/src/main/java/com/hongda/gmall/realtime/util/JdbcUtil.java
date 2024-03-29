package com.hongda.gmall.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.hongda.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * select count(*) from t;                                  单行单列
 * select count(*) from t group by dept_id;                 多行单列
 * select * from t;                                         单行多列
 * select dept_id,count(*) from t group by dept_id;         多行多列
 */
public class JdbcUtil {

    //针对所有JDBC服务的所有查询
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //创建集合用于存放结果数据
        ArrayList<T> list = new ArrayList<>();

        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询操作
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        //遍历查询到的结果集,将每行数据封装为T对象,并将其放入集合
        while (resultSet.next()) {  /*行遍历*/

            //创建T对象
            T t = clz.newInstance(); /*反射创建对象*/

            for (int i = 0; i < columnCount; i++) {  /*列遍历*/
                //获取列名
                String columnName = metaData.getColumnName(i + 1);
                //获取列值
                Object value = resultSet.getObject(columnName);

                //判断是否需要转换列名信息
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());/*转换列名为驼峰式*/
                }

                //给T对象赋值
                /*
                * 当你调用BeanUtils.setProperty(t, columnName, value)时，
                * BeanUtils会尝试找到一个合适的方法来设置columnName和value。由于JSONObject是一个动态的键值对容器，
                * BeanUtils会找到并调用JSONObject的put()方法，
                * 将columnName作为键，value作为值添加到JSONObject对象中。这样，BeanUtils实际上是在运行时为JSONObject对象动态地添加了键值对。
                * */
                BeanUtils.setProperty(t, columnName, value); /*程序过程中, 动态为对象赋值*/
            }

            //将T对象加入集合
            list.add(t);
        }

        //释放资源
        resultSet.close();
        preparedStatement.close();

        //返回结果数据
        return list;
    }

    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        System.out.println(queryList(connection,
                "select count(*) ct from gmall2022_realtime.DIM_BASE_TRADEMARK",
                JSONObject.class,
                true));

        connection.close();
    }

}
