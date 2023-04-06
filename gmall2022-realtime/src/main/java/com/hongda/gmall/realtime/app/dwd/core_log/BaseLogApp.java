package com.hongda.gmall.realtime.app.dwd.core_log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hongda.gmall.realtime.util.DateFormatUtil;
import com.hongda.gmall.realtime.util.MyKafkaUtilHongda;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app -> nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：  Mock -> f1.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
/*
知识点:
1. keybystream的状态编程
2. 侧输出流的使用
    使用侧输出流对主流做分流.
 */
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生成环境设置为Kafka主题的分区数

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop101:9810/tmp");

        //TODO 2.读取Kafka topic_log 主题的数据创建流
//        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer("ods_log", "base_log_app_2022"));
        DataStreamSource<String> kafkaDS = env.fromSource(MyKafkaUtilHongda.getKafkaSource("ods_log", "base_log_app_2022"), WatermarkStrategy.noWatermarks(), "ods_log_Source");
        kafkaDS.print("kafkasource>>");

        //TODO 3.将数据转换为JSON格式,并过滤掉非JSON格式的数据
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>");
        //本来是要落盘脏数据, 这里偷懒了.
        dirtyDS.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String value) throws Exception {
                return null;
            }
        });

        //TODO 4.使用状态编程做新老用户校验. 即验证依据数据对新老用户标签进行检查以及修正.
        //以json字符串中mid字段进行keyby
        /*
        问题
        1) java方法在什么情况下, 可以采用函数式编程?
        java方法有@FunctionalInterface标签, 才可以使用函数式编程
         */
        KeyedStream<JSONObject, String> keyedByMidStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        keyedByMidStream.print("keyby");

        /*
        问题
        1) 为什么要用RichMapFunction, 而不是普通的map函数??
            因为涉及到状态编程, 需要自定义状态部分的处理逻辑
        2) 不存在聚合逻辑, 为什么要用keyby.
            因为涉及到状态编程, 需要以key分组,  为每组分配一个唯一key state. 组内每处理1条数据, 都要用到组内共享的key state.
         */

        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedByMidStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //1.获取"is_new"标记&获取状态数据
                String isNew = value.getJSONObject("common").getString("is_new");
                String lastVisitDt = lastVisitDtState.value();
                Long ts = value.getLong("ts");

                //2.判断是否为"1"
                if ("1".equals(isNew)) {

                    //3.获取当前数据的时间
                    String curDt = DateFormatUtil.toDate(ts);

                    if (lastVisitDt == null) {
                        lastVisitDtState.update(curDt);
                    } else if (!lastVisitDt.equals(curDt)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }

                } else if (lastVisitDt == null) {
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    lastVisitDtState.update(yesterday);
                }

                return value;
            }
        });
        jsonObjWithNewFlagDS.print("jsonObjWithNewFlagDS");

        //TODO 5.使用侧输出流对数据进行分流处理
        // 页面浏览: 主流
        // 启动日志：侧输出流
        // 曝光日志：侧输出流
        // 动作日志：侧输出流
        // 错误日志：侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };
        /*
        为什么要用ProcessFunction?

         */
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                String jsonString = value.toJSONString();

                //尝试取出数据中的Error字段
                String error = value.getString("err");
                if (error != null) {
                    //输出数据到错误日志
                    ctx.output(errorTag, jsonString);
                }

                //尝试获取启动字段
                String start = value.getString("start");
                if (start != null) {
                    //输出数据到启动日志
                    ctx.output(startTag, jsonString);
                } else {

                    //取出页面id与时间戳
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    String common = value.getString("common");

                    //尝试获取曝光数据
                    //1) 压平曝光数据数组
                    //2) 为曝光数据, 补充父级字段.
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            display.put("common", common);

                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取动作数据
                    //1) 压平动作数据数组
                    //2) 为动作数据, 补充父级字段.
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("page_id", pageId);
                            action.put("ts", ts);
                            action.put("common", common);
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    //输出数据到页面浏览日志
                    //页面日志json数据, 删除曝光和动作相关字段.
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });

        //TODO 6.提取各个数据的数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        //TODO 7.将各个流的数据分别写出到Kafka对应的主题中
        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.sinkTo(MyKafkaUtilHongda.getKafkaSink(env,page_topic));
        startDS.sinkTo(MyKafkaUtilHongda.getKafkaSink(env,start_topic));
        errorDS.sinkTo(MyKafkaUtilHongda.getKafkaSink(env,error_topic));
        displayDS.sinkTo(MyKafkaUtilHongda.getKafkaSink(env,display_topic));
        actionDS.sinkTo(MyKafkaUtilHongda.getKafkaSink(env,action_topic));

        //TODO 8.启动
        env.execute("BaseLogApp");

    }

}
