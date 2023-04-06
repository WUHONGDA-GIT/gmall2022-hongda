package com.hongda.gmall.realtime.demo.app.transfrom;


import com.hongda.gmall.realtime.demo.bean.Bean1;
import com.hongda.gmall.realtime.demo.bean.Bean2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
/*
知识点:
    1. 设置空闲状态过期时间.
    2. UpdateType中 OnCreateAndWrite 与OnReadAndWrite的区别
        OnCreateAndWrite: 状态被创建时或被改写时, 更新过期时间. 访问, 不更新过期时间.
        OnReadAndWrite: 状态被创建时或被改写,或被访问时, 更新过期时间.
    2. 多表全外连接的坑.
 */
/**
 * @Date: 2022-10-01 15:13
 * @Description: 通过nc, 实验Flink的运行原理, 需要特别注意, join左右两边状态过期时间对join的影响.
 */
public class FlinkSQLJoinDemo {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention()); // 默认0S, 表示空间状态不过期
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10)); //空闲状态最长保留时间.

        //读取数据创建流
        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop101", 7777)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop101", 8888)
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
//        tableEnv.sqlQuery("select t1.id,t1.name,t2.sex from t1 join t2 on t1.id = t2.id")
//                .execute()
//                .print();

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

        env.execute();

    }

}
