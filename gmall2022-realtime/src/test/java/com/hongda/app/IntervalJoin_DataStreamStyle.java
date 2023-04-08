package com.hongda.app;
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


public class IntervalJoin_DataStreamStyle {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);




        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop101", 8888)
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

        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop101", 7777)
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
        /*
        此处状态管理的逻辑,
         */
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> result = bean1DS.keyBy(Bean1::getId)
                /*
                这里，我们使用 Bean1::getId 作为一个函数，表示对于每个 Bean1 对象，我们希望使用该对象的 getId() 方法的返回值作为匿名函数的返回值;
                 */
                .intervalJoin(bean2DS.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
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
