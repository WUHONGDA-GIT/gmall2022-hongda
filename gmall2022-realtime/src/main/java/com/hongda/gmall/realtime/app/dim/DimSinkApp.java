package com.hongda.gmall.realtime.app.dim;
import akka.remote.WireFormats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.hongda.gmall.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4;//与kafka一致

        // TODO 2. 检查点相关设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, WireFormats.TimeUnit.DAYS), Time.of(3L, WireFormats.TimeUnit.MINUTES)));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop101:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 读取业务主流
        //3.1 声明消费主题以及消费者组
        String topic = "topic_db";
        String groupId = "dim_sink_app";
        //3.2 获取消费者对象
        FlinkKafkaConsumer<String> kafkaConsumer = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        //3.3 消费数据 得到流
        DataStreamSource<String> dbStrDS = env.addSource(kafkaConsumer);

        // TODO 4. 对读取的流中数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = dbStrDS.map(JSON::parseObject);

        // TODO 5. 主流 ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        try {
                            jsonObj.getJSONObject("data");
                            if (jsonObj.getString("type").equals("bootstrap-start")
                                    || jsonObj.getString("type").equals("bootstrap-complete")) {
                                return false;
                            }
                            return true;
                        } catch (JSONException jsonException) {
                            return false;
                        }
                    }
                });
        // 打印测试
        filterDS.print("filterDS >>> ");
        env.execute();
    }
}