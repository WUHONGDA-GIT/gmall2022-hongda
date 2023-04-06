package com.hongda.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hongda.gmall.realtime.app.func.TableProcessFunction;
import com.hongda.gmall.realtime.bean.TableProcess;
import com.hongda.gmall.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
/*
1. 广播流与主流的connect
    利用配置广播流, 对主流做数据处理
2. BroadcastConnectedStream的处理案例.
 */
public class DimSinkApp {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生成环境设置为Kafka主题的分区数

//        env.setStateBackend(new HashMapStateBackend());
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");


        //TODO 2.读取 Kafka topic_db 主题数据创建流
//        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtilHongda.getKafkaConsumer("ods_db", "dim_app_2022"));
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer("ods_db", "dim_app_2022_2"));

        //TODO 3.过滤掉非JSON格式的数据,并将其写入侧输出流
        //创建侧输出流
        OutputTag<String> dirtyDataTag = new OutputTag<String>("Dirty") {
        };

        /*
        为什么要用process?
        因为get到context, 从而将数据写入侧输出流
         */
        SingleOutputStreamOperator<JSONObject> jsonObjDS
                = kafkaDS.process(new ProcessFunction<String, JSONObject>() { // 泛型说明, <输入数据类型, 输出数据类型>
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);//转换json对象失败, 就会抛出异常.
                    collector.collect(jsonObject); //每个map任务收集自己的结果, 并各自发送下游的算子;
                } catch (Exception e) {
                    context.output(dirtyDataTag, s);//捕捉异常数据, 写入侧输出流
                }
            }
        });

        //取出脏数据并打印
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
        sideOutput.print("Dirty>>>>>>>>>>");

        //TODO 4.使用FlinkCDC读取MySQL中的配置信息
        /*
        Flink的MysqlSource 为什么要用josn反序列化器?
            flinkCDC 会以json字符串形式, 发送捕捉到的mysql的变动记录
         */

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2022_config")
                .tableList("gmall2022_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial()) //选择数据同步的起点.
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        //TODO 5.将配置信息流处理成广播流
        /*
        将mysqlSourceDS(目标维表list)以map形式广播出去.
        问题:
        1) map key 和 value分别是什么?
            key = sourceTable来源表名, value = tableprocess bean对象
        2) 为什么要用数据结构map???
            相当于一张hash表, 未来主流数据可以在O(1)内, 判断当前数据是否属于目标维表数据, 如果属于, 则写入hbase
         */
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
                = new MapStateDescriptor<>("map-state", String.class, TableProcess.class); // key.class, value.class
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);


        //TODO 6.连接主流与广播流
        /*
        汇总2条流, 转换成BroadcastConnectedStream
        BroadcastConnectedStream<JSONObject, String> : 泛型1 = 流1数据类型,泛型2 = 流2数据类型
         */
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);


        //TODO 7.根据广播流数据处理主流数据
        /*
        处理ConnectedStream中的数据, 转换成一条普通流
        思考: 为什么process要传入BroadcastProcessFunction??
            因为调用process函数的func是BroadcastConnectedStream
         */
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        //TODO 8.将数据写出到Phoenix中
        //思考
        //为什么不能用jdbc? jdbc只适合往单表里面写, 不适合写多表
        //为什么要继承richFunc, 因为要在进程环境启动时, 提前建立好连接, 所以会用到open方法
        hbaseDS.print(">>>>>>>>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        //TODO 9.启动任务
        env.execute("DimApp");

    }
}