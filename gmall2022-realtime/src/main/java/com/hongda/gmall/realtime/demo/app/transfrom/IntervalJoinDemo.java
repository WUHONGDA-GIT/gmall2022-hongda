package com.hongda.gmall.realtime.demo.app.transfrom;

import com.hongda.gmall.realtime.demo.bean.Bean1;
import com.hongda.gmall.realtime.demo.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
/**
 * @Date: 2022-10-01 15:13
 * @Description: 通过nc, 实验IntervalJoin的运行原理, 需要特别注意, 合流之后水位线合并问题, 橙色流开的桶关闭时机问题.
 */
public class IntervalJoinDemo {
    /*
    橙色
    1001,male,15
    1001,male,21
    蓝色
    1001,male,10
    1001,male,20
    1001,male,21
     */
    public static void main(String[] args) throws Exception {
        /*
        nc -lk 7777
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Bean1> orangeDS = env.socketTextStream("hadoop101", 7777)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                    @Override
                    public long extractTimestamp(Bean1 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));
        /*
        nc -lk 8888
         */
        SingleOutputStreamOperator<Bean2> buleDS = env.socketTextStream("hadoop101", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //双流JOIN
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> result = orangeDS.keyBy(Bean1::getId)
                .intervalJoin(buleDS.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5)) //默认左右闭区间
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 left, Bean2 right, Context ctx, Collector<Tuple2<Bean1, Bean2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        //打印结果并启动
        result.print(">>>>>>>>");
        env.execute();


    }

}
