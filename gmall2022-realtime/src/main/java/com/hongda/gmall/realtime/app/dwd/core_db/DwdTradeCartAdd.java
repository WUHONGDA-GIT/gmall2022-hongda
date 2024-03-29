package com.hongda.gmall.realtime.app.dwd.core_db;

import com.hongda.gmall.realtime.util.MyKafkaUtil;
import com.hongda.gmall.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
/*

 */
public class DwdTradeCartAdd {
    /*
    知识点
    1) 为什么使用flinkSQl, 而不使用flinkDatasteam??
        flinkDatasteam 只支持内连接, 其他左外, 全外连接要自己实现, 十分麻烦, 也容易出错.
        flinkSQL, 提供直接的api, 因此, 针对结构化的处理逻辑, 使用SQL实现, 会更快 更安全, 更简单
    2) 为什么使用jdbc lookup??
        可以直接从mysql退化维度到 dwd层, 而不需要先做一张维表到dim层, 再将维表信息, 退化到事实表中.
    3) 在事实表, join 维表时, 如何实现维表数据不过期, 事实表数据正常过期?
    */
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        //设置状态存储时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //TODO 2.使用DDL方式读取Kafka ods_db 主题数据
        /*
        注意是db数据, 因此无论是哪个表, json字段都是一样的, 因为表字段被封装成了json, 放进了data/old字段中;
         */
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_cart_add_2022"));

        //打印测试
//        Table table = tableEnv.sqlQuery("select * from ods_db");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print(">>>>>>>>>>>");

        //TODO 3.过滤出加购数据
        /*
        只统计加购动作,  删购动作忽略;
         */
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['cart_price'] cart_price, " +
                "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
                "    data['sku_name'] sku_name, " +
                "    data['is_checked'] is_checked, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['is_ordered'] is_ordered, " +
                "    data['order_time'] order_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    pt " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='cart_info' " +
                "and (`type`='insert'  " +
                "    or (`type`='update'  " +
                "        and `old`['sku_num'] is not null " +
                "        and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        tableEnv.createTemporaryView("cart_add", cartAddTable);

        //打印测试
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(cartAddTable, Row.class);
//        rowDataStream.print(">>>>>>>>>>>");
//
        //TODO 4.读取MySQL中的base_dic表构建维表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        //打印测试
//        Table table = tableEnv.sqlQuery("select * from base_dic");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print(">>>>>>>>>>>");
//
        //TODO 5.关联两张表   维度退化
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    c.id, " +
                "    c.user_id, " +
                "    c.sku_id, " +
                "    c.cart_price, " +
                "    c.sku_num, " +
                "    c.sku_name, " +
                "    c.is_checked, " +
                "    c.create_time, " +
                "    c.operate_time, " +
                "    c.is_ordered, " +
                "    c.order_time, " +
                "    c.source_type, " +
                "    c.source_id, " +
                "    b.dic_name " +
                "from cart_add c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as b " +
                "on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        //打印测试
        //Table table = tableEnv.sqlQuery("select * from result_table");
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        //rowDataStream.print(">>>>>>>>>>>");

        //TODO 6.将数据写回到Kafka DWD层
        /*可以注意到, 处理时间pt, 并没有落到kafka*/
        String sinkTopic = "dwd_trade_cart_add";
        tableEnv.executeSql("" +
                "create table dwd_trade_cart_add( " +
                "    id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    cart_price string, " +
                "    sku_num int, " +
                "    sku_name string, " +
                "    is_checked string, " +
                "    create_time string, " +
                "    operate_time string, " +
                "    is_ordered string, " +
                "    order_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    dic_name string " +
                ")" + MyKafkaUtil.getKafkaDDL(sinkTopic, ""));
        tableEnv.executeSql("insert into dwd_trade_cart_add select * from result_table")
                .print(); //行动算子,不能剩;

//        //TODO 7.启动任务
        env.execute("DwdTradeCartAdd");

    }

}
