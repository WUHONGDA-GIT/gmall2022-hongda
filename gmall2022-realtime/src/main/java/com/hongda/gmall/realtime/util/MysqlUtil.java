package com.hongda.gmall.realtime.util;

public class MysqlUtil {

    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`( " +
                "    `dic_code` string, " +
                "    `dic_name` string, " +
                "    `parent_code` string, " +
                "    `create_time` timestamp, " +
                "    `operate_time` timestamp, " +
                "    primary key(`dic_code`) not enforced " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop101:3306/gmall2022', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = '123456', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

}
