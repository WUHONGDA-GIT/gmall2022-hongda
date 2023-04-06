package com.hongda.gmall.realtime.app.dwd.core_log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.hongda.gmall.realtime.util.DateFormatUtil;
import com.hongda.gmall.realtime.util.MyKafkaUtil;
import com.hongda.gmall.realtime.util.MyKafkaUtilHongda;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//数据流：web/app -> Nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：  Mock -> f1.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
/*
知识点:
    1) 状态过期时间的设置, UpdateType中OnCreateAndWrite 与OnReadAndWrite的区别
 */
public class DwdTrafficUniqueVisitorDetail {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生成环境设置为Kafka主题的分区数

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");

        //TODO 2.读取 Kafka dwd_traffic_page_log 主题数据创建流
        String sourceTopic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail_211027";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                return JSON.parseObject(value);
//            }
//        });

        //TODO 4.过滤掉上一跳页面id不等于null的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        });

        //TODO 5.按照Mid分组
        KeyedStream<JSONObject, String> keyedByMidStream = filterDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 6.使用状态编程进行每日登录数据去重
        //TODO 遗留问题: 是否涉及到状态编程, 就要使用Rcih函数?
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedByMidStream.filter(new RichFilterFunction<JSONObject>() {
            /*只要jvm进程不关掉, keyedByMidStream.filter就不会关闭, 那么visitDtState状态对象就不会被回收, 状态一致有效;*/
            private ValueState<String> visitDtState;

            /*
            open方法会在每个分区的第一个数据元素到达之前调用，即在每个分区的第一个数据元素被过滤之前调用。
            open方法只会在实例化RichFilterFunction对象时调用一次，而不是在每个分区的第一个数据元素到达时都调用。如果你想在每个数据元素到达时执行某些操作，可以在实现的filter方法中进行。
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-dt", String.class);
                /*
                设置状态的TTL
                 */
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                //初始化状态
                visitDtState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出状态以及当前数据的日期
                String dt = visitDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                //如果状态数据为null或者状态日期与当前数据日期不同,则保留数据,同时更新状态,否则弃之
                /*
                !dt.equals(curDt), 24小时内再次访问的, 访问时间更新至最新的, 同时由于UpdateType.OnCreateAndWrite, 因此会更新状态存活时间;
                 */
                if (dt == null || !dt.equals(curDt)) {
                    visitDtState.update(curDt);
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 7.将数据写出到Kafka
        uvDetailDS.print(">>>>>>>>>");
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDetailDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(targetTopic));

        //TODO 8.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");

    }

}
