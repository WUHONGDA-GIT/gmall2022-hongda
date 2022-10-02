package com.hongda.gmall.realtime.util;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;


public class MyKafkaUtil {
    private static String BOOTSTRAP_SERVERS = "hadoop101:9092, hadoop102:9092, hadoop103:9092";
    private static String DEFAULT_TOPIC = "default_topic";
    private static Properties properties = new Properties();

    static{
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }
    /**
     * 创建1个消费者
     * @param topic 消费topic
     * @param groupId 消费者id
     * @return 1个kafka消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
        /*
        自定义反序列化器原因
            为什么不直接使用 new SimpleStringSchema()?
            因为: SimpleStringSchema CLASS->deserialize()->String()->形参@NotNull -> 传入数据不能为null. 但实际数据是有可能为null的.
        自定义反序列化器是实现思路:
            仿照SimpleStringSchema. 只不过补充了null值处理逻辑
         */
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
                }, properties);
        return consumer;
    }


    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                //消费起始位移选择之前所提交的偏移量（如果没有，则重置为最新,即从最大的offset开始消费）
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //消费起始位移选择之前所提交的偏移量（如果没有，则重置为最早,即从 0 offset开始消费）
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }

                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if(message != null ) {
                            return new String(message);
                        }
                        return "";
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }
                }).build();
       return source;
    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }



//
    public static KafkaSink<String> getKafkaSink(StreamExecutionEnvironment env, String topic) {
        /*
        https://ververica.zendesk.com/hc/en-us/articles/360013269680-Best-Practices-for-Using-Kafka-Sources-Sinks-in-Flink-Jobs
        If you configure your Flink Kafka producer with end-to-end exactly-once semantics, it is strongly recommended to configure the Kafka transaction timeout to a duration longer than the maximum checkpoint duration plus the maximum expected Flink job downtime.
        遗留问题: 事务和检查点执行关系是怎么样的?
            初步理解: 检查点完成之后, 事务才能完成. 所以如果 事务超时time < 检查点超时time, 那么检查点永远没可能完成.
         */
        //TODO 设置检查点超时时间
        env.enableCheckpointing(1000); //防止事务超时导致checkpiont失败, checkpoint间隔时间要远远小于事务超时时间
        env.getCheckpointConfig().setCheckpointTimeout(3600000);//单位millisecond //防止事务超时导致checkpiont失败, checkpoint超时时间要小于事务超时时间

        //TODO 设置事务超时时间
        /*
        transaction timeout = maximum checkpoint duration + the maximum expected Flink job downtime
         */
        Properties properties = new Properties();// unit is millisecond
        properties.setProperty("transaction.timeout.ms", "7200000"); // e.g., 2 hours

        //TODO 创建KafkaSink, 实现精准一次性消费.
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setKafkaProducerConfig(properties)
//                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("KafkaSinkDemo") //使用在整个flink集群内, 唯一的前缀, 此处采用类名. BUG点: 如此设计不同团队之间可能会冲突.
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        return sink;
    }
}
