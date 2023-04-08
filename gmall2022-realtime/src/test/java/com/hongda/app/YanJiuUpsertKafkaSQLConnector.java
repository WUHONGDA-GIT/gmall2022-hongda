package com.hongda.app;

import com.hongda.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class YanJiuUpsertKafkaSQLConnector {

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
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(3));

        //TODO 2.使用DDL方式读取 Kafka ods_db 主题的数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_order_detail_211027"));
//        tableEnv.executeSql(MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail_211027"));


//        //TODO 3.过滤出订单明细数据
        Table orderDetailTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] order_detail_id, " +
                "    data['order_id'] order_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['sku_name'] sku_name, " +
                "    data['order_price'] order_price, " +
                "    data['sku_num'] sku_num, " +
                "    data['create_time'] create_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    cast(cast(data['sku_num'] as decimal(16,2)) * cast(data['order_price'] as decimal(16,2)) as String) split_original_amount, " +
                "    data['split_total_amount'] split_total_amount, " +
                "    data['split_activity_amount'] split_activity_amount, " +
                "    data['split_coupon_amount'] split_coupon_amount, " +
                "    pt " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_detail' " +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail", orderDetailTable);

        //打印测试
//        Table table = tableEnv.sqlQuery("select * from order_detail");
//        tableEnv.toAppendStream(table, Row.class).print(">>>>>>>>>");
//        tableEnv.toChangelogStream(table).print(">>>>>>>>>");
//        tableEnv.toRetractStream(table, Row.class).print(">>>>>>>>>");

        //TODO 4.过滤出订单数据
        Table orderInfoTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['consignee'] consignee, " +
                "    data['consignee_tel'] consignee_tel, " +
                "    data['total_amount'] total_amount, " +
                "    data['order_status'] order_status, " +
                "    data['user_id'] user_id, " +
                "    data['payment_way'] payment_way, " +
                "    data['out_trade_no'] out_trade_no, " +
                "    data['trade_body'] trade_body, " +
                "    data['operate_time'] operate_time, " +
                "    data['expire_time'] expire_time, " +
                "    data['process_status'] process_status, " +
                "    data['tracking_no'] tracking_no, " +
                "    data['parent_order_id'] parent_order_id, " +
                "    data['province_id'] province_id, " +
                "    data['activity_reduce_amount'] activity_reduce_amount, " +
                "    data['coupon_reduce_amount'] coupon_reduce_amount, " +
                "    data['original_total_amount'] original_total_amount, " +
                "    data['feight_fee'] feight_fee, " +
                "    data['feight_fee_reduce'] feight_fee_reduce, " +
                "    `type`, " +
                "    `old` " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_info' " +
                "and (`type`='insert' or `type`='update')");
        tableEnv.createTemporaryView("order_info", orderInfoTable);

        //打印测试
//        Table table1 = tableEnv.sqlQuery("select * from order_info");
//        tableEnv.toAppendStream(table1, Row.class).print(">>>>>>>>>");

        //TODO 8.关联5张表
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    od.order_detail_id, " +
                "    od.order_id, " +
                "    oi.user_id " +
                "from order_detail od " +

                "left join order_info oi " +
                "on od.order_id = oi.id " );
        tableEnv.createTemporaryView("result_table", resultTable);

//        打印测试
//        Table table2 = tableEnv.sqlQuery("select * from result_table");
//        tableEnv.toChangelogStream(table2).print(">>>>>>>>>");/*打印撤回流; */
//        tableEnv.toAppendStream(table2, Row.class).print(">>>>>>>>>"); //报错 //因为left join 出来的是撤回流

        //TODO 9.创建Kafka upsert-kafka表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail_test( " +
                "    `order_detail_id` string, " +
                "    `order__id` string, " +
                "    `user_id` string, " +
                "    PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_detail_test"));

        //TODO 10.将数据写出
        tableEnv.executeSql("insert into dwd_trade_order_detail_test select * from result_table").print();


        //TODO 11.启动任务
        env.execute("DwdTradeOrderDetail");




    }

}
