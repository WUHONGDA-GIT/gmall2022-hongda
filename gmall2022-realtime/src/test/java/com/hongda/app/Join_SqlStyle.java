package com.hongda.app;


import com.hongda.gmall.realtime.demo.bean.Bean1;
import com.hongda.gmall.realtime.demo.bean.Bean2;
import com.hongda.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Join_SqlStyle {
    /*
    注意: 以下是基于处理事件的sql join, 通过设置状态过期, 来达到窗口过期的效果;
    实际生产中, 不会这样写的, 这个代码很不符合生产; 生产中不会使用状态TTL来控制窗口的长度, 因为状态TTL只能基于处理时间;
    而实际生产中, 大多情况, 是基于事件时间, 通过join on 来完成关联窗口的长度设置;
     */
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //I设置状态过期时间, 相同与设置nterval join中, 元素关联的时间上下界
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //读取数据创建流
        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop101", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop101", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        //将流转换为动态表
        tableEnv.createTemporaryView("t1", bean1DS);
        tableEnv.createTemporaryView("t2", bean2DS);

        //内连接       左表：OnCreateAndWrite   右表：OnCreateAndWrite
        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id = t2.id")
                .execute()
                .print();

        //左外连接     左表：OnReadAndWrite     右表：OnCreateAndWrite
//        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        //右外连接     左表：OnCreateAndWrite   右表：OnReadAndWrite
//        tableEnv.sqlQuery("select t2.id,t1.name,t2.sex from t1 right join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        //全外连接     左表：OnReadAndWrite     右表：OnReadAndWrite
//        tableEnv.sqlQuery("select t1.id,t2.id,t1.name,t2.sex from t1 full join t2 on t1.id = t2.id")
//                .execute()
//                .print();

        Table table = tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 left join t2 on t1.id = t2.id");
//        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
//        retractStream.print(">>>>>>>>>");
        tableEnv.createTemporaryView("t", table);

        /*left join 实现过程
        假设A表作为主表与B表做等值左外联。
        当A表数据进入算子，而B表数据未至时会先生成一条B表字段均为null的关联数据ab1，其标记为 +I。
        其后，B表数据到来，会先将之前的数据撤回，即生成一条与ab1内容相同，但标记为-D的数据，再生成一条关联后的数据，标记为 +I。
        这样生成的动态表对应的流称之为回撤流。
        这里如果将left join 之后的数据写到kafka主题的话，会有null值,我们不能使用kafka connector，需要使用kafka upsert connector；

        如果你非要使用kafka connector,
        使用new FlinkKafkaConsumer<String>("topic",new SimpleStringSchema(),prop) 消费数据的话,
        SimpleStringSchema进行序列化的时候，要求value不能为空，会报错；所以我们自己重写反序列化方法。

        */
        //创建Kafka表
        tableEnv.executeSql("" +
                "create table result_table(" +
                "    id string," +
                "    name string," +
                "    sex string," +
                "    PRIMARY KEY (id) NOT ENFORCED " +
                ") " + MyKafkaUtil.getUpsertKafkaDDL("test"));

        //将数据写入Kafka
        tableEnv.executeSql("insert into result_table select * from t")
                .print();

        env.execute();

    }

}
