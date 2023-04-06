package com.hongda.gmall.realtime.demo.app.source;

import com.hongda.gmall.realtime.util.MyKafkaUtilHongda;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Date: 2022-10-02 10:49
 * @Description: 分别使用KafkaSource 和 FlinkKafkaConsumer 消费topic , 观察输出行为的差异性.
 * 注意: 官网表示,  FlinkKafkaConsumer将会在1.15被弃用.
 * FlinkKafkaConsumer is deprecated and will be removed with Flink 1.15, please use KafkaSource instead.
 */
public class KafkaSourceAndKafkaConsumerDemo  {
    public static void main(String[] args)  throws Exception{
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生成环境设置为Kafka主题的分区数

        //TODO 2.读取 Kafka topic_db 主题数据创建流
        DataStreamSource<String> kafkaDS1 = env.addSource(MyKafkaUtilHongda.getKafkaConsumer("ods_db", "dim_app_2022_1"));
        DataStreamSource<String> kafkaDS2 = env.fromSource(MyKafkaUtilHongda.getKafkaSource("ods_db", "dim_app_2022_2"), WatermarkStrategy.noWatermarks(), "ods_db_Source");

        kafkaDS1.print("version111111");
        kafkaDS2.print("version222222");
        //TODO 3.过滤掉非JSON格式的数据,并将其写入侧输出流
        env.execute();
    }
}
