package com.hongda.gmall.realtime.app.dwd.core_db.ez;


import com.hongda.gmall.realtime.util.MyKafkaUtil;
import lombok.val;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdTradeCancelDetail {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 1.获取执行环境_带WebUI
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081"); // 指定访问端口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //env.setStateBackend(new HashMapStateBackend());
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        //TODO 2.使用DDL的方式读取 Kafka dwd_trade_order_detail 主题的数据
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail_table( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_order_cancel_211027"));

        //TODO 3.过滤出取消订单数据
        Table filterTable = tableEnv.sqlQuery("" +
                "select " +
                "    * " +
                "from dwd_trade_order_detail_table " +
                "where `type`='update' " +
                "and `old`['order_status'] is not null " +
                "and order_status='1003'");
        tableEnv.createTemporaryView("filter_table", filterTable);



        //TODO 4.创建Kafka下单数据表
        tableEnv.executeSql("" +
                "create table dwd_trade_cancel_detail_table ( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_cancel_detail", ""));

        //TODO 5.将数据写出到Kafka
        tableEnv.executeSql("insert into dwd_trade_cancel_detail_table select * from filter_table");

        filterTable.execute().print();//只要使用相同的table api行动算子, 就可以看到控制台的输出了;

//        /*
//        * 为什么看不到控制台的内容?
//        * 由于 Table API 和 DataStream API 的执行环境不同，
//        * 所以 tableEnv.executeSql() 中的写入 Kafka 操作和 DataStream API 中的 print() 操作实际上是在两个不同的任务中执行的。
//        * 这就是为什么您看不到控制台输出的原因。
//        * */
//        //TODO 打印到控制台
//        Table table = tableEnv.sqlQuery("select * from filter_table");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print(">>>>>>>>>>>");

        //TODO 7.启动任务
        env.execute("DwdTradeCancelDetail");

    }
}
