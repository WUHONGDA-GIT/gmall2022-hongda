package com.hongda.gmall.realtime.app.sourcecase;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Date: 2022-10-01 05:48
 * @Description:
 */
public class FlinkCDCDataStream {
    /*
    flink CDC 发送的数据
    {"before":null,"after":{"source_table":"base_category1","sink_table":"dim_base_category1","sink_columns":"id,name"
    ,"sink_pk":"id","sink_extend":null},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source"
    ,"ts_ms":1664594964927,"snapshot":"false","db":"gmall2022_config","sequence":null,"table":"table_process","server_id":0
    ,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1664594964927,"transaction":null}
     */
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生成环境设置为Kafka主题的分区数

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

        mysqlSourceDS.print();

        env.execute();

    }
}
