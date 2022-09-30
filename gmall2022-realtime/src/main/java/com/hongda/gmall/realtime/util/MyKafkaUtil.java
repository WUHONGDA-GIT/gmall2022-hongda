package com.hongda.gmall.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class MyKafkaUtil {
    private static String BOOTSTRAP_SERVERS = "hadoop101:9092, hadoop102:9092, hadoop103:9092";
    private static String DEFAULT_TOPIC = "default_topic";

    //用途: 创建1个kafka消费者
    //参数: 消费topic, 消费者id
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
        //自定义反序列化器原因
                // 为什么不直接使用 new SimpleStringSchema()
                // 因为: SimpleStringSchema CLASS->deserialize()->String()->传入的参数@NotNull, 不能为null. 实际数据是有可能为null的.
        //自定义反序列化器思路:
                //仿照SimpleStringSchema. 只不过补充了null值处理逻辑

        new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }
                    @Override

                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if(record != null && record.value() != null) {
                            return new String(record.value());
                        }
                        return "";
                    }
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                }, prop);
        return consumer;
    }
}
